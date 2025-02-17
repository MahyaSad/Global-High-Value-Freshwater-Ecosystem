import rioxarray
import geopandas as gpd
from tqdm import tqdm
import os
import pandas as pd
import dask.array as da
import numpy as np
import dask.dataframe as dd
from rasterio.windows import Window
import logging
import psutil
from osgeo import osr
import numpy as np
import os
import logging
from tqdm import tqdm
from osgeo import osr
from osgeo import gdal
import gc
import time
import psutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_utm_zone(bounds):
    """
    Calculate the most appropriate UTM zone based on country centroid.
    """
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    
    # Calculate UTM zone number
    zone_number = int((center_lon + 180) / 6) + 1
    
    # Determine hemisphere for complete EPSG code
    if center_lat >= 0:
        # Northern hemisphere
        epsg = f"EPSG:{32600 + zone_number}"
    else:
        # Southern hemisphere
        epsg = f"EPSG:{32700 + zone_number}"
    
    return epsg

def get_equal_area_projection(bounds):
    """
    Select the most appropriate equal-area projection based on country location and extent.
    """
    min_lat, max_lat = bounds[1], bounds[3]
    min_lon, max_lon = bounds[0], bounds[2]
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2
    extent_lat = max_lat - min_lat
    extent_lon = max_lon - min_lon
    
    # Special case for Brazil
    if (-35 < min_lat < 6 and -75 < min_lon < -30 and 
        extent_lat > 20 and extent_lon > 20):
        return "EPSG:5641"  # South America Albers Equal Area Conic - optimized for Brazil
    
    # Special case for Pacific Islands (including Fiji)
    if (160 < max_lon or min_lon < -160) and -30 < center_lat < 30:
        return get_utm_zone(bounds)  # Use UTM for Pacific islands
    
    # For regions near equator (between 15°S and 15°N)
    if min_lat >= -15 and max_lat <= 15:
        if extent_lon < 20:  # Small to medium sized countries
            return get_utm_zone(bounds)
        else:  # Wider countries like Indonesia
            return "ESRI:54009"  # World Mollweide
    
    # For regions in northern hemisphere
    elif center_lat > 15:
        if center_lat > 60:  # High latitude
            if center_lon < 0:
                return "EPSG:3573"  # North America Arctic
            else:
                return "EPSG:3575"  # Arctic LAEA
        else:  # Mid latitudes
            if -100 < center_lon < -30:  # North America
                return "EPSG:5070"  # North America Albers
            elif 0 < center_lon < 90:  # Europe/Asia
                return "EPSG:3035"  # ETRS89-LAEA Europe
            else:  # Asia
                return "EPSG:3577"  # GDA94 / Australian Albers
    
    # For regions in southern hemisphere
    else:
        if center_lat < -60:  # Antarctica
            return "EPSG:3031"  # Antarctic Polar Stereographic
        elif -60 < center_lon < -30:  # South America
            return "EPSG:5641"  # South America Albers Equal Area Conic
        elif 110 < center_lon < 160:  # Australia
            return "EPSG:3577"  # GDA94 / Australian Albers
        else:  # Africa and other regions
            return "ESRI:54009"  # World Mollweide as fallback
    
    return "ESRI:54009"  # World Mollweide as final fallback



def calculate_pixel_area(ds_proj, lat):
    """
    Calculate the actual pixel area taking into account latitude if using geographic coordinates.
    
    Parameters:
    ds_proj: xarray Dataset with projection information
    lat: float, latitude in degrees
    
    Returns:
    float: pixel area in square meters
    """
    if ds_proj.rio.crs.is_geographic:
        # For geographic coordinates, adjust for latitude
        # Earth's radius in meters
        R = 6371000
        
        # Calculate actual ground distances
        dy = np.abs(ds_proj.rio.resolution()[0]) * (np.pi/180.0) * R
        dx = np.abs(ds_proj.rio.resolution()[1]) * (np.pi/180.0) * R * np.cos(np.radians(lat))
        
        return dx * dy
    else:
        # For projected coordinates, simple multiplication is fine
        return abs(ds_proj.rio.resolution()[0] * ds_proj.rio.resolution()[1])

def process_window(bounds, country_tif, classes=[1, 2, 3,4, 5, 6, 7, 8]):
    """Process a single window of the raster data with improved projection handling"""
    try:
        window_areas = {cls: 0 for cls in classes}
        
        with rioxarray.open_rasterio(country_tif) as ds:
            # Slice the data using coordinates
            ds_window = ds.sel(
                x=slice(bounds[0], bounds[2]),
                y=slice(bounds[3], bounds[1])
            )
            
            if ds_window.size == 0:
                return None
                
            # Get appropriate projection
            proj_crs = get_equal_area_projection(bounds)
            
            # Reproject window
            ds_proj = ds_window.rio.reproject(
                proj_crs,
                resolution=30  # Keep resolution in meters
            )
            
            # Calculate center latitude for area calculations
            center_lat = (bounds[1] + bounds[3]) / 2
            
            # Calculate pixel area with latitude correction
            pixel_area = calculate_pixel_area(ds_proj, center_lat)
            
            # Calculate areas for each class
            for cls in classes:
                if isinstance(ds_proj.data, da.Array):
                    class_pixels = (ds_proj.data == cls).sum().compute()
                else:
                    class_pixels = np.sum(ds_proj.data == cls)
                
                area = float(pixel_area * class_pixels)
                window_areas[cls] = area / 10000  # Convert to hectares
                
        return bounds, window_areas
            
    except Exception as e:
        logger.error(f"Error processing window {bounds}: {str(e)}")
        return None



def get_projection_wkt(proj_crs):
    """Convert projection identifier to WKT format"""
    out_srs = osr.SpatialReference()
    
    if proj_crs.startswith('EPSG:'):
        if proj_crs == "EPSG:2193":  # New Zealand Transverse Mercator 2000
            nztm_wkt = '''PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",
                GEOGCS["NZGD2000",
                    DATUM["New_Zealand_Geodetic_Datum_2000",
                        SPHEROID["GRS 1980",6378137,298.257222101,
                            AUTHORITY["EPSG","7019"]],
                        TOWGS84[0,0,0,0,0,0,0],
                        AUTHORITY["EPSG","6167"]],
                    PRIMEM["Greenwich",0,
                        AUTHORITY["EPSG","8901"]],
                    UNIT["degree",0.0174532925199433,
                        AUTHORITY["EPSG","9122"]],
                    AUTHORITY["EPSG","4167"]],
                PROJECTION["Transverse_Mercator"],
                PARAMETER["latitude_of_origin",0],
                PARAMETER["central_meridian",173],
                PARAMETER["scale_factor",0.9996],
                PARAMETER["false_easting",1600000],
                PARAMETER["false_northing",10000000],
                UNIT["metre",1,
                    AUTHORITY["EPSG","9001"]],
                AXIS["Easting",EAST],
                AXIS["Northing",NORTH],
                AUTHORITY["EPSG","2193"]]'''
            out_srs.ImportFromWkt(nztm_wkt)
        elif proj_crs == "EPSG:5641":  # South America Albers Equal Area Conic
            sa_wkt = '''PROJCS["South_America_Albers_Equal_Area_Conic",
                GEOGCS["GCS_South_American_1969",
                    DATUM["D_South_American_1969",
                        SPHEROID["GRS_1967_Truncated",6378160.0,298.25]],
                    PRIMEM["Greenwich",0.0],
                    UNIT["Degree",0.0174532925199433]],
                PROJECTION["Albers_Conic_Equal_Area"],
                PARAMETER["False_Easting",0.0],
                PARAMETER["False_Northing",0.0],
                PARAMETER["Central_Meridian",-54.0],
                PARAMETER["Standard_Parallel_1",-5.0],
                PARAMETER["Standard_Parallel_2",-42.0],
                PARAMETER["Latitude_Of_Origin",-32.0],
                UNIT["Meter",1.0]]'''
            out_srs.ImportFromWkt(sa_wkt)
        else:
            out_srs.ImportFromEPSG(int(proj_crs.split(':')[1]))
    elif proj_crs == "ESRI:54009":  # World Mollweide
        mollweide_wkt = '''PROJCS["World_Mollweide",
            GEOGCS["GCS_WGS_1984",
                DATUM["WGS_1984",
                    SPHEROID["WGS_1984",6378137.0,298.257223563]],
                PRIMEM["Greenwich",0.0],
                UNIT["Degree",0.0174532925199433]],
            PROJECTION["Mollweide"],
            PARAMETER["False_Easting",0.0],
            PARAMETER["False_Northing",0.0],
            PARAMETER["Central_Meridian",0.0],
            UNIT["Meter",1.0]]'''
        out_srs.ImportFromWkt(mollweide_wkt)
    elif proj_crs == "EPSG:3035":  # ETRS89-LAEA Europe
        out_srs.ImportFromEPSG(3035)
    elif proj_crs == "EPSG:3573":  # North America Arctic
        out_srs.ImportFromEPSG(3573)
    elif proj_crs == "EPSG:3575":  # Arctic LAEA
        out_srs.ImportFromEPSG(3575)
    elif proj_crs == "EPSG:5070":  # North America Albers
        out_srs.ImportFromEPSG(5070)
    elif proj_crs == "EPSG:3577":  # GDA94 Australian Albers
        out_srs.ImportFromEPSG(3577)
    elif proj_crs == "EPSG:3031":  # Antarctic Polar Stereographic
        out_srs.ImportFromEPSG(3031)
    else:
        raise ValueError(f"Unsupported projection: {proj_crs}")
        
    out_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return out_srs.ExportToWkt()

def calculate_country_riparian_area(folder_path, window_size=1.0):
    """
    Calculate riparian area for a country using GDAL for efficient reprojection.
    
    Parameters:
    folder_path (str): Path to the folder containing country TIFF files
    window_size (float): Size of processing window in degrees (default: 1.0)
    
    Returns:
    dict: Dictionary containing areas for each class in hectares, or zero-filled dict if no data
    """
    try:
        # Configure GDAL
        gdal.UseExceptions()
        gdal.SetConfigOption('GDAL_CACHEMAX', '1024')  # Set cache size to 1GB
        
        # Find all TIFF files in the folder
        tiff_files = [f for f in os.listdir(folder_path) if f.endswith('.tif')]
        
        # Initialize total areas dictionary for the country with zeros
        country_total_areas = {cls: 0 for cls in [1, 2, 3,4, 5, 6, 7, 8]}
        
        # Return zero-filled dictionary if no TIFF files found
        if not tiff_files:
            logger.info(f"No TIFF files found in {folder_path}, returning zero areas")
            return country_total_areas
            
        logger.info(f"Found {len(tiff_files)} TIFF files to process")
        
        # Create process-specific temp directory
        temp_dir = os.path.join(folder_path, f"temp_{os.getpid()}")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Process each TIFF file
        for tiff_file in tiff_files:
            logger.info(f"\nProcessing file: {tiff_file}")
            input_tif = os.path.join(folder_path, tiff_file)
            temp_output = os.path.join(temp_dir, f"reprojected_{os.getpid()}_{tiff_file}")
            
            try:
                # Track memory usage
                process = psutil.Process(os.getpid())
                mem_start = process.memory_info().rss / 1024 / 1024
                logger.info(f"Memory usage at start: {mem_start:.2f} MB")
                
                # Open the input dataset
                src_ds = gdal.Open(input_tif, gdal.GA_ReadOnly)
                if src_ds is None:
                    logger.error(f"Could not open {input_tif}")
                    continue
                
                # Get the input bounds
                gt = src_ds.GetGeoTransform()
                minx = gt[0]
                maxy = gt[3]
                maxx = gt[0] + gt[1] * src_ds.RasterXSize
                miny = gt[3] + gt[5] * src_ds.RasterYSize
                
                bounds = (minx, miny, maxx, maxy)
                
                # Get appropriate projection
                proj_crs = get_equal_area_projection(bounds)
                logger.info(f"Using projection {proj_crs}")
                
                try:
                    dst_wkt = get_projection_wkt(proj_crs)
                except ValueError as e:
                    logger.error(str(e))
                    continue
                
                # Set up optimized warp options
                warp_options = gdal.WarpOptions(
                    format='GTiff',
                    srcSRS='EPSG:4326',
                    dstSRS=dst_wkt,
                    xRes=30,
                    yRes=30,
                    resampleAlg=gdal.GRA_NearestNeighbour,
                    multithread=True,
                    warpMemoryLimit=1024,  # 1GB memory limit
                    creationOptions=[
                        'COMPRESS=LZW',
                        'PREDICTOR=2',
                        'BIGTIFF=YES',
                        'TILED=YES',
                        'BLOCKXSIZE=256',
                        'BLOCKYSIZE=256'
                    ],
                    callback=gdal.TermProgress_nocb
                )
                
                # Perform the reprojection
                logger.info("Reprojecting raster...")
                ds_warped = gdal.Warp(temp_output, src_ds, options=warp_options)
                
                # Clean up source dataset
                src_ds = None
                gc.collect()
                
                if ds_warped is None:
                    logger.error(f"Failed to reproject {tiff_file}")
                    continue
                
                # Verify data validity
                band = ds_warped.GetRasterBand(1)
                sample_data = band.ReadAsArray(0, 0, min(1024, ds_warped.RasterXSize), 
                                             min(1024, ds_warped.RasterYSize))
                if sample_data is None:
                    logger.error(f"Failed to read sample data from {tiff_file}")
                    continue
                
                # Clean up sample data
                del sample_data
                gc.collect()
                
                # Get pixel resolution
                gt_rep = ds_warped.GetGeoTransform()
                pixel_area = abs(gt_rep[1] * gt_rep[5])  # in square meters
                
                # Initialize areas for this file
                file_areas = {cls: 0 for cls in [1, 2, 3,4, 5, 6, 7, 8]}
                
                # Process by blocks
                block_sizes = band.GetBlockSize()
                x_block_size = min(1024, block_sizes[0])
                y_block_size = min(1024, block_sizes[1])
                
                total_blocks = ((ds_warped.RasterYSize + y_block_size - 1) // y_block_size) * \
                             ((ds_warped.RasterXSize + x_block_size - 1) // x_block_size)
                
                last_mem_check = time.time()
                
                with tqdm(total=total_blocks, desc="Processing blocks") as pbar:
                    for y in range(0, ds_warped.RasterYSize, y_block_size):
                        # Monitor memory usage every 30 seconds
                        if time.time() - last_mem_check > 30:
                            mem_usage = process.memory_info().rss / 1024 / 1024
                            logger.info(f"Current memory usage: {mem_usage:.2f} MB")
                            last_mem_check = time.time()
                            gc.collect()
                            
                        rows = min(y_block_size, ds_warped.RasterYSize - y)
                        for x in range(0, ds_warped.RasterXSize, x_block_size):
                            cols = min(x_block_size, ds_warped.RasterXSize - x)
                            
                            data = band.ReadAsArray(x, y, cols, rows)
                            
                            # Calculate areas for each class
                            for cls in file_areas.keys():
                                pixels = np.sum(data == cls)
                                area = float(pixel_area * pixels)
                                file_areas[cls] += area / 10000  # Convert to hectares
                            
                            # Clean up block data
                            del data
                            pbar.update(1)
                
                # Clean up
                band = None
                ds_warped = None
                gc.collect()
                
                # Remove temporary file
                if os.path.exists(temp_output):
                    try:
                        os.remove(temp_output)
                    except Exception as e:
                        logger.warning(f"Could not remove temp file: {e}")
                
                # Log results for this file
                file_total = sum(file_areas.values())
                logger.info(f"Total area for {tiff_file}: {file_total:.2f} Ha")
                
                # Add file areas to country totals
                for cls in country_total_areas:
                    country_total_areas[cls] += file_areas[cls]
                
                # Log memory usage after file
                mem_end = process.memory_info().rss / 1024 / 1024
                logger.info(f"Memory usage after file: {mem_end:.2f} MB")
                    
            except Exception as e:
                logger.error(f"Error processing file {tiff_file}: {str(e)}")
                if os.path.exists(temp_output):
                    try:
                        os.remove(temp_output)
                    except Exception as cleanup_e:
                        logger.warning(f"Could not remove temp file: {cleanup_e}")
                continue
        
        # Clean up temp directory
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.warning(f"Could not remove temp directory: {e}")
        
        # Log final results
        country_total = sum(country_total_areas.values())
        logger.info(f"\nFinal country totals:")
        logger.info(f"Total area: {country_total:.2f} Ha")
        for cls in country_total_areas:
            logger.info(f"Class {cls}: {country_total_areas[cls]:.2f} Ha")
        
        return country_total_areas
            
    except Exception as e:
        logger.error(f"Error processing folder {folder_path}: {str(e)}")
        # Return zero-filled dictionary instead of None on error
        return {cls: 0 for cls in [1, 2, 3,4, 5, 6, 7, 8]}

def main():
    """Main function with enhanced reporting and individual country file saving"""
    results_dir = "result_max"
    output_dir = "country_results_max"
    final_output_csv = "all_countries_riparian_areas_max.csv"
    window_size = 2.0
    
    # Define countries to skip
    countries_to_skip = {'USA_total','Kiribati_1'}
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all country folders
    country_folders = [d for d in os.listdir(results_dir) 
                      if os.path.isdir(os.path.join(results_dir, d))]
    country_folders = country_folders[::-1]  # Reverse order
    
    results = []
    logger.info(f"Found {len(country_folders)} countries to process")
    
    try:
        for country in tqdm(country_folders, desc="Processing countries"):
            # Skip specified countries
            if country in countries_to_skip:
                logger.info(f"Skipping {country} - in skip list")
                continue
                
            country_output_file = os.path.join(output_dir, f"{country}_riparian_areas.csv")
            
            # Check if country has already been processed
            if os.path.exists(country_output_file):
                logger.info(f"Loading existing results for {country}")
                existing_result = pd.read_csv(country_output_file).to_dict('records')[0]
                results.append(existing_result)
                continue
                
            folder_path = os.path.join(results_dir, country)
            logger.info(f"\nProcessing country: {country}")
            
            # Count TIFF files
            tiff_count = len([f for f in os.listdir(folder_path) if f.endswith('.tif')])
            logger.info(f"Number of TIFF files: {tiff_count}")
            
            # Process country - will return dictionary with zeros if no data
            areas = calculate_country_riparian_area(folder_path, window_size)
            
            # Create result dictionary
            result = {
                'Country': country,
                'Number_of_Tiffs': tiff_count,
                'Class_1_Area_Ha': round(areas[1], 2),
                'Class_2_Area_Ha': round(areas[2], 2),
                'Class_3_Area_Ha': round(areas[3], 2),
                'Class_4_Area_Ha': round(areas[4], 2),
                'Class_5_Area_Ha': round(areas[5], 2),
                'Class_6_Area_Ha': round(areas[6], 2),
                'Class_7_Area_Ha': round(areas[7], 2),
                'Class_8_Area_Ha': round(areas[8], 2),
                'Total_Area_Ha': round(sum(areas.values()), 2)
            }
            
            # Always append the result, even if all areas are zero
            results.append(result)
            
            # Save individual country results
            pd.DataFrame([result]).to_csv(country_output_file, index=False)
            logger.info(f"Saved results for {country} to {country_output_file}")
            
            logger.info(f"Results for {country}:")
            print(pd.DataFrame([result]))
        
        # Create final DataFrame with all results
        if results:
            df = pd.DataFrame(results)
            
            # Ensure we're keeping all countries, including those with zero values
            # Sort by Total_Area_Ha but preserve zero values
            df = df.sort_values('Total_Area_Ha', ascending=False, na_position='last')
            
            # Save all results, including zeros
            df.to_csv(final_output_csv, index=False)
            logger.info(f"\nFinal results saved to: {final_output_csv}")
            
            # Print summary statistics
            logger.info("\nTIFF file statistics:")
            logger.info(f"Average TIFFs per country: {df['Number_of_Tiffs'].mean():.1f}")
            logger.info(f"Max TIFFs in a country: {df['Number_of_Tiffs'].max()}")
            logger.info(f"Min TIFFs in a country: {df['Number_of_Tiffs'].min()}")
            
            logger.info("\nSummary Statistics:")
            logger.info(f"Total countries processed: {len(df)}")
            logger.info(f"Total riparian area: {df['Total_Area_Ha'].sum():,.2f} Ha")
            logger.info(f"Countries with zero riparian area: {len(df[df['Total_Area_Ha'] == 0])}")
            
            # Show both top 5 and bottom 5 countries
            logger.info("\nTop 5 countries by riparian area:")
            print(df[['Country', 'Number_of_Tiffs', 'Total_Area_Ha']].head())
            
            logger.info("\nBottom 5 countries (including zeros):")
            print(df[['Country', 'Number_of_Tiffs', 'Total_Area_Ha']].tail())
    
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    print('s')
    main()