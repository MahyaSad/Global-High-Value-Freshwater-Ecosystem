/* 
==============================================
Aspiration to Action: Opportunities to Align Freshwater Ecosystems with Climate Strategies
==============================================
Date Created/Added: 11/18/2024
Date Updated: 05/23/2025
==============================================
Authors: Vivian Griffey
=================================================================================================
Script Description:
This script can be used to generate estimates of annual carbon accumulation potential from forest restoration
for different HVFE classes across countries
=================================================================================================
Datasets used:
(1) Cook-Patton et al. 2020 Carbon Accumulation Rates in Natural Regeneration https://doi.org/10.1038/s41586-020-2686-x
(2) HVFE delineation
(3) UMD Land Cover Land Use Change 2020 https://glad.umd.edu/dataset/GLCLUC2020
(3) Dinerstein et al. 2017 Biomes https://ecoregions.appspot.com/
(4) Natural Earth countries https://www.naturalearthdata.com/downloads/10m-cultural-vectors/
=================================================================================================
Expected Outputs:
(1) Tables of annual carbon accumulation by HVFE class by country
=================================================================================================

=================================================================================================
IMPORTANT USAGE NOTE:
-This script should be used to calculate carbon accumulation totals for classes 3-4 in the minimum scenario,
as these incorporate maximum tree cover limits. The script HVFE should be used to calculate carbon accumulation totals for
the maximum scenario (HVFE classes 3-6) and for class 5 in the minimum scenario. 
=================================================================================================
*/


////////////////////////
// FOREST BIOMES ONLY //
////////////////////////

var biomes = ee.FeatureCollection("projects/ee-vsgriffey/assets/expertRecs_lessComplex_biomes") //Dinerstein
print(biomes.first())
var forests = biomes.filter(ee.Filter.lt('BIOME_NUM', 6));
Map.addLayer(forests)

var countries = ee.FeatureCollection("projects/ee-vsgriffey/assets/ne_10m_admin_0_countries");


//////////
// HVFE //
//////////


// Class 1: surface water
// Class 2: regularly flooded wetlands
// Class 3: headwater regions
// Class 4: fixed‐width buffer around low‐order stream buffer
// Class 5: fixed‐width buffer around surface water and high‐Order Streams
// Class 6: geomorphic floodplains (included only in maximum scenario)
// Class 7: fixed‐width buffer around regularly flooded wetlands

//need to select classes 3 4 and 5 for both scenarios and 6 for max
// for applying TIA thresholds, only dealing with classes 3 and 4 for the min scenario

// MINIMUM //
var fwc1 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile1_V2").clipToCollection(forests)
var fwc2 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile2_V2").clipToCollection(forests)
var fwc3 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile3_V2").clipToCollection(forests)
var fwc4 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile4_V2").clipToCollection(forests)
var fwc5 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile5_V2").clipToCollection(forests)
var fwc6 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile6_V2").clipToCollection(forests)
var fwc7 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile7_V2").clipToCollection(forests)
var fwc8 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile8_V2").clipToCollection(forests)
var fwc9 = ee.Image("projects/ee-vsgriffey/assets/riparian_full_min_tile9_V2").clipToCollection(forests)

var fwc_inForest = ee.ImageCollection([fwc1, fwc2, fwc3, fwc4, fwc5, fwc6, fwc7, fwc8, fwc9]).mosaic();
Map.addLayer(fwc_inForest)
//var fwc_inForest = fwc.clipToCollection(forests)

var fwc_mask = ee.Image(0)
    .where(fwc_inForest.eq(3)
    .or(fwc_inForest.eq(4)),1)
    .remap([1], [1], null, 'constant');
Map.addLayer(fwc_mask, {}, "Headwaters, buffers around headwater streams and rivers, floodplains")

var class3_mask = ee.Image(0)
    .where(fwc_inForest.eq(3), 1)
    .remap([1], [1], null, 'constant');
Map.addLayer(class3_mask, {palette:"red"}, "headwaters")
    
var class4_mask = ee.Image(0)
    .where(fwc_inForest.eq(4), 1)
    .remap([1], [1], null, 'constant');
Map.addLayer(class4_mask, {palette:"blue"}, "fixed‐width buffer around low‐order stream buffer")


//////////////////////////////////
// PREP COOK PATTON CARBON DATA //
//////////////////////////////////

var cookpatton = ee.Image("projects/ci_geospatial_assets/restoration/carbon_accumulation_gap_filled")
var fwc_carbon = cookpatton.updateMask(fwc_mask);

///////////////////////////
// PREP UMD LULC DATASET //
///////////////////////////

//No need for restoring tree cover, water, wetlands, built up, true desert classes…. 
//<<simplest solution: restore all others>> 

var landmask = ee.Image("projects/glad/OceanMask").lte(1)
var m20 = ee.Image('projects/glad/GLCLU2020/v2/LCLUC_2020').updateMask(landmask);
//Map.addLayer(m20,visParamMap,'2020 land cover and land use')

//2-24 = short veg
//244 = cropland


var shortVeg_mask = m20.gte(2).and(m20.lte(24));
var crop_mask = m20.eq(244)
//unlike other script, we keep shortVeg and crop separated because maximum tree cover values differ between them

// Apply the mask to the image, setting the values in the specified ranges to null (masked out)
//var fwc_carbon_LCLU = fwc_carbon.updateMask(combinedMask.not()); // Mask out the ranges (invert the mask)
var fwc_carbon_LCLU_crop = fwc_carbon.updateMask(crop_mask);
var fwc_carbon_LCLU_shortVeg = fwc_carbon.updateMask(shortVeg_mask);

///////////////////////////////////
// CALCULATE CARBON ACCUMULATION // 
///////////////////////////////////

//create a mask to use for each class
var class3_mask_carbon_crop = fwc_carbon_LCLU_crop.updateMask(class3_mask);
var class3_mask_carbon_shortVeg = fwc_carbon_LCLU_shortVeg.updateMask(class3_mask);

var class4_mask_carbon_crop = fwc_carbon_LCLU_crop.updateMask(class4_mask);
var class4_mask_carbon_shortVeg = fwc_carbon_LCLU_shortVeg.updateMask(class4_mask);

var areaImage = ee.Image.pixelArea();


//////////////////////
// PREP EXPERT RECS //
//////////////////////

var expertRecs = ee.FeatureCollection("projects/ee-vsgriffey/assets/expertRecs_lessComplex")

//Rasterize
var recs_r_crop = expertRecs
  .reduceToImage({
    properties: ["TIA1"],
    reducer: ee.Reducer.first()
  });
  
var recs_r_shortVeg = expertRecs
  .reduceToImage({
    properties: ["TIA2"],
    reducer: ee.Reducer.first()
  });
//var recs_r_crops2 = recs_r_crops.selfMask();


/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////

var CarbonAcc_pixel_yr_class3_crop = class3_mask_carbon_crop //units C/ha/yr
.multiply(recs_r_crop).divide(100)
.multiply(areaImage)
.divide(10000)// multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.multiply(30) //is annual, go to 30 years
//.selfMask();
//.unmask(-9999);

var CarbonAcc_pixel_yr_class3_shortVeg = class3_mask_carbon_shortVeg //units C/ha/yr
.multiply(recs_r_shortVeg).divide(100)
.multiply(areaImage)
.divide(10000)// multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr

var CarbonAcc_pixel_yr_class3 = ee.ImageCollection([CarbonAcc_pixel_yr_class3_crop, CarbonAcc_pixel_yr_class3_shortVeg]).mosaic();
Map.addLayer(CarbonAcc_pixel_yr_class3, {palette:["#ffeda0","#feb24c","#f03b20"], min:0, max:3000}, "Annual carbon accumulation in headwaters all") //final units C/pixel/yr


//add up carbon accumulation across countries or biomes
var CarbonAcc_pixel_yr_class3_min_sum = CarbonAcc_pixel_yr_class3.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});
//print('Carbon Accumulation in headwaters in forests in MX basin: ', CarbonAcc_pixel_yr_headwaters_sum)

// export the table to drive
Export.table.toDrive({
    collection: CarbonAcc_pixel_yr_class3_min_sum,
    description: 'CarbonAcc_pixel_yr_class3_min_sum_TIA_countries',
    fileNamePrefix: 'CarbonAcc_pixel_yr_class3_min_sum_TIA_countries',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});



var CarbonAcc_pixel_yr_class4_crop = class4_mask_carbon_crop //units C/ha/yr
.multiply(recs_r_crop).divide(100)
.multiply(areaImage)
.divide(10000)// multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.multiply(30) //is annual, go to 30 years
//.selfMask();
//.unmask(-9999);

var CarbonAcc_pixel_yr_class4_shortVeg = class4_mask_carbon_shortVeg //units C/ha/yr
.multiply(recs_r_shortVeg).divide(100)
.multiply(areaImage)
.divide(10000)// multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr

var CarbonAcc_pixel_yr_class4 = ee.ImageCollection([CarbonAcc_pixel_yr_class4_crop, CarbonAcc_pixel_yr_class4_shortVeg]).mosaic();

//add up carbon accumulation across MX
var CarbonAcc_pixel_yr_class4_min_sum = CarbonAcc_pixel_yr_class4.reduceRegions({
    collection:countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

// export the table to drive
Export.table.toDrive({
    collection: CarbonAcc_pixel_yr_class4_min_sum,
    description: 'CarbonAcc_pixel_yr_class4_min_sum_TIA_countries',
    fileNamePrefix: 'CarbonAcc_pixel_yr_class4_min_sum_TIA_countries',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});


