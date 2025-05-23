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
-This script should be used to calculate carbon accumulation totals for the maximum scenario (HVFE classes 3-6)
and for class 5 in the minimum scenario. The HVFE2 script should be used to calculate carbon accumulation totals for
classes 3-4 in the minimum scenario, as those incorporate maximum tree cover limits. 
-Users should comment in/out the classes in this script when going between min and max scenarios. Be sure to change table export names
when changing between scenarios.
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

// MAXIMUM //
// var fwc1 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile1_V2").clipToCollection(forests)
// var fwc2 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile2_V2").clipToCollection(forests)
// var fwc3 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile3_V2").clipToCollection(forests)
// var fwc4 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile4_V2").clipToCollection(forests)
// var fwc5 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile5_V2").clipToCollection(forests)
// var fwc6 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile6_V2").clipToCollection(forests)
// var fwc7 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile7_V2").clipToCollection(forests)
// var fwc8 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile8_V2").clipToCollection(forests)
// var fwc9 = ee.Image("projects/ee-vsgriffey/assets/riparian_max_tile9_V2").clipToCollection(forests)

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


var fwc_mask = ee.Image(0)
    .where(fwc_inForest.eq(3)
    .or(fwc_inForest.eq(4))
    .or(fwc_inForest.eq(5)),1)
    //.or(fwc_inForest.eq(6)),1) //make sure to comment or uncomment this depending on scenario
    .remap([1], [1], null, 'constant');

var class3_mask = ee.Image(0)
    .where(fwc_inForest.eq(3), 1)
    .remap([1], [1], null, 'constant');
Map.addLayer(class3_mask, {palette:"red"}, "headwaters")
    
var class4_mask = ee.Image(0)
    .where(fwc_inForest.eq(4), 1)
    .remap([1], [1], null, 'constant');
Map.addLayer(class4_mask, {palette:"blue"}, "fixed‐width buffer around low‐order stream buffer")

var class5_mask = ee.Image(0)
    .where(fwc_inForest.eq(5), 1)
    .remap([1], [1], null, 'constant');
Map.addLayer(class5_mask, {palette:"green"}, "fixed‐width buffer around surface water and high‐Order Streams")

// var class6_mask = ee.Image(0) //make sure to comment or uncomment this depending on scenario
//     .where(fwc_inForest.eq(6), 1)
//     .remap([1], [1], null, 'constant');
// Map.addLayer(class6_mask, {palette:"yellow"}, "geomorphic floodplains")


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
Map.addLayer(m20,visParamMap,'2020 land cover and land use')

var shortVeg_mask = m20.gte(2).and(m20.lte(24));
var crop_mask = m20.eq(244)
var combinedMask = shortVeg_mask.or(crop_mask)

// Apply the mask to the image, setting the values in the specified ranges to null (masked out)
var fwc_carbon_LCLU = fwc_carbon.updateMask(combinedMask); // Mask out the ranges (invert the mask)
Map.addLayer(fwc_carbon_LCLU, {},'freshwater carbon in forest biomes in restoration target LULC')


///////////////////////////////////
// CALCULATE CARBON ACCUMULATION // 
///////////////////////////////////


//create a mask to use for each class
var class3_mask_carbon = fwc_carbon_LCLU.updateMask(class3_mask);
var class4_mask_carbon = fwc_carbon_LCLU.updateMask(class4_mask);
var class5_mask_carbon = fwc_carbon_LCLU.updateMask(class5_mask);
//var class6_mask_carbon = fwc_carbon_LCLU.updateMask(class6_mask); //make sure to comment or uncomment this depending on scenario
var areaImage = ee.Image.pixelArea();

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////
var CarbonAcc_pixel_yr_class3 = class3_mask_carbon //units C/ha/yr
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.multiply(30) //is annual, go to 30 years
//.selfMask();
//.unmask(-9999);
Map.addLayer(CarbonAcc_pixel_yr_class3, {palette:["#ffeda0","#feb24c","#f03b20"], min:0, max:3000}, "Annual carbon accumulation in headwaters") //final units C/pixel/yr


//add up carbon accumulation across countries
var CarbonAcc_pixel_yr_class3_max_sum = CarbonAcc_pixel_yr_class3.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});
//print('Carbon Accumulation in headwaters in forests in MX basin: ', CarbonAcc_pixel_yr_headwaters_sum)


// export the table to drive
Export.table.toDrive({
    collection: CarbonAcc_pixel_yr_class3_max_sum, //ee.FeatureCollection([CarbonAcc_pixel_yr_headwaters_sum]),
    description: 'CarbonAcc_pixel_yr_class3_max_sum',
    fileNamePrefix: 'CarbonAcc_pixel_yr_class3_max_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO_A3","NAME"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});


var CarbonAcc_pixel_yr_class4 = class4_mask_carbon //units C/ha/yr
.multiply(areaImage) //
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.multiply(30) //is annual, go to 30 years
//.selfMask();
//.unmask(-9999);
//Map.addLayer(CarbonAcc_pixel_yr_class4, {palette:["#ffeda0","#feb24c","#f03b20"], min:0, max:3000}, "Annual carbon accumulation in not headwaters")//final units C/pixel/yr


//add up carbon accumulation
var CarbonAcc_pixel_yr_class4_max_sum = CarbonAcc_pixel_yr_class4.reduceRegions({
    collection:countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});
//print('Carbon Accumulation in not headwaters in forests in MX basin: ', CarbonAcc_pixel_yr_not_headwaters_sum)

// export the table to drive
Export.table.toDrive({
    collection: CarbonAcc_pixel_yr_class4_max_sum,
    description: 'CarbonAcc_pixel_yr_class4_max_sum',
    fileNamePrefix: 'CarbonAcc_pixel_yr_class4_max_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});


var CarbonAcc_pixel_yr_class5 = class5_mask_carbon //units C/ha/yr
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.multiply(30) //is annual, go to 30 years
//.selfMask();
//.unmask(-9999);
Map.addLayer(CarbonAcc_pixel_yr_class5, {palette:["#ffeda0","#feb24c","#f03b20"], min:0, max:3000}, "Annual carbon accumulation in surface water buffers") //final units C/pixel/yr


//add up carbon accumulation across countries
var CarbonAcc_pixel_yr_class5_min_sum = CarbonAcc_pixel_yr_class5.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

// export the table to drive
Export.table.toDrive({
    collection: CarbonAcc_pixel_yr_class5_min_sum, //ee.FeatureCollection([CarbonAcc_pixel_yr_headwaters_sum]),
    description: 'CarbonAcc_pixel_yr_class5_min_sum',
    fileNamePrefix: 'CarbonAcc_pixel_yr_class5_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});

//make sure to comment or uncomment this depending on scenario
// var CarbonAcc_pixel_yr_class6 = class6_mask_carbon //units C/ha/yr
// .multiply(areaImage) //
// .divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
// //.multiply(30) //is annual, go to 30 years
// //.selfMask();
// //.unmask(-9999);
// //Map.addLayer(CarbonAcc_pixel_yr_class4, {palette:["#ffeda0","#feb24c","#f03b20"], min:0, max:3000}, "Annual carbon accumulation in not headwaters")//final units C/pixel/yr


// //add up carbon accumulation
// var CarbonAcc_pixel_yr_class6_max_sum = CarbonAcc_pixel_yr_class6.reduceRegions({
//     collection:countries,
//     reducer:ee.Reducer.sum(),
//     scale: 30,
//     crs: 'EPSG:4326'
// });
// //print('Carbon Accumulation in not headwaters in forests in MX basin: ', CarbonAcc_pixel_yr_not_headwaters_sum)

// // export the table to drive
// Export.table.toDrive({
//     collection: CarbonAcc_pixel_yr_class6_max_sum,
//     description: 'CarbonAcc_pixel_yr_class6_max_sum',
//     fileNamePrefix: 'CarbonAcc_pixel_yr_class6_max_sum',
//     selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
//     fileFormat: 'CSV'
// });




//////////////////////////////////
// CALCULATE Forested HVFE AREA //
//////////////////////////////////
var total_area_class3 = class3_mask
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();


//add up area 
var forested_area_ha_class3_min_sum = total_area_class3.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: forested_area_ha_class3_min_sum,
    description: 'forested_area_ha_class3_min_sum',
    fileNamePrefix: 'forested_area_ha_class3_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});


var total_area_class4 = class4_mask
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();


var forested_area_ha_class4_min_sum = total_area_class4.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: forested_area_ha_class4_min_sum,
    description: 'forested_area_ha_class4_min_sum',
    fileNamePrefix: 'forested_area_ha_class4_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});

var total_area_class5 = class5_mask
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();


var forested_area_ha_class5_min_sum = total_area_class5.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: forested_area_ha_class5_min_sum,
    description: 'forested_area_ha_class5_min_sum',
    fileNamePrefix: 'forested_area_ha_class5_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});

//make sure to comment or uncomment this depending on scenario
// var total_area_class6 = class6_mask
// .multiply(areaImage) 
// .divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
// //.selfMask();


// var forested_area_ha_class6_max_sum = total_area_class6.reduceRegions({
//     collection: countries,
//     reducer:ee.Reducer.sum(),
//     scale: 30,
//     crs: 'EPSG:4326'
// });

// Export.table.toDrive({
//     collection: forested_area_ha_class6_max_sum,
//     description: 'forested_area_ha_class6_max_sum',
//     fileNamePrefix: 'forested_area_ha_class6_max_sum',
//     selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
//     fileFormat: 'CSV'
// });



////////////////////////////////
// CALCULATE RESTORABLE AREA ///
////////////////////////////////


var class3_mask_carbon_area = ee.Image(0)
    .where(class3_mask_carbon.gt(0), 1)
    .remap([1], [1], null, 'constant')
var class4_mask_carbon_area = ee.Image(0)
    .where(class4_mask_carbon.gt(0), 1)
    .remap([1], [1], null, 'constant')
var class5_mask_carbon_area = ee.Image(0)
    .where(class5_mask_carbon.gt(0), 1)
    .remap([1], [1], null, 'constant')
// var class6_mask_carbon_area = ee.Image(0) //make sure to comment or uncomment this depending on scenario
//     .where(class6_mask_carbon.gt(0), 1)
//     .remap([1], [1], null, 'constant')


var restorable_area_class3 = class3_mask_carbon_area
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();

//add up area 
var restorable_area_class3_min_sum = restorable_area_class3.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: restorable_area_class3_min_sum,
    description: 'restorable_area_class3_min_sum',
    fileNamePrefix: 'restorable_area_class3_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});


var restorable_area_class4 = class4_mask_carbon_area
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();


var restorable_area_class4_max_sum = restorable_area_class4.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: restorable_area_class4_max_sum,
    description: 'restorable_area_class4_max_sum',
    fileNamePrefix: 'restorable_area_class4_max_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});



var restorable_area_class5 = class5_mask_carbon_area
.multiply(areaImage) 
.divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
//.selfMask();


var restorable_area_class5_min_sum = restorable_area_class5.reduceRegions({
    collection: countries,
    reducer:ee.Reducer.sum(),
    scale: 30,
    crs: 'EPSG:4326'
});

Export.table.toDrive({
    collection: restorable_area_class5_min_sum,
    description: 'restorable_area_class5_min_sum',
    fileNamePrefix: 'restorable_area_class5_min_sum',
    selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
    fileFormat: 'CSV'
});

//make sure to comment or uncomment this depending on scenario
// var restorable_area_class6 = class6_mask_carbon_area
// .multiply(areaImage) 
// .divide(10000) // multiplying by areaImage and dividing by 10000 (sqm to ha) is going from C/ha/yr to C/pixel/yr
// //.selfMask();


// var restorable_area_class6_max_sum = restorable_area_class6.reduceRegions({
//     collection: countries,
//     reducer:ee.Reducer.sum(),
//     scale: 30,
//     crs: 'EPSG:4326'
// });

// Export.table.toDrive({
//     collection: restorable_area_class6_max_sum,
//     description: 'restorable_area_class6_max_sum',
//     fileNamePrefix: 'restorable_area_class6_max_sum',
//     selectors: (["ISO_A3","NAME", "sum"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
//     fileFormat: 'CSV'
// });


// var CarbonAcc_ha_yr_class5_min_mean = class5_mask_carbon.reduceRegion({
//     geometry: testPol,
//     reducer:ee.Reducer.mean(),
//     scale: 30,
//     crs: 'EPSG:4326',
//     maxPixels: 1e13
// });


// Export.table.toDrive({
//     collection: CarbonAcc_ha_yr_class5_min_mean,
//     description: 'CarbonAcc_ha_yr_class5_min_mean',
//     fileNamePrefix: 'CarbonAcc_ha_yr_class5_min_mean',
//     selectors: (["mean"]), //(["ISO3","COUNTRY","GID_0","NAME_0","sum"]),//"BIOME_NAME","BIOME_NUM",
//     fileFormat: 'CSV'
// });
