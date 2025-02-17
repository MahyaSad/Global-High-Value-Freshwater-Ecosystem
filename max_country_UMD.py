import os
import numpy as np
import rioxarray as rxr
import logging
import csv
from osgeo import gdal
from collections import defaultdict
import gc
import time
from functools import partial
import multiprocessing as mp
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
import glob

class TiffProcessor:
    def __init__(self):
        # Configure logging
        self.logger = self._setup_logger()
        
        # Configure GDAL
        self._configure_gdal()
        
        # Define UMD classes
        self.UMD_CLASSES = {
            "11-75% Short Vegetation": list(range(2, 20)),
            "79-100% Short Vegetation": list(range(19, 25)) + [240],
            "Tree Cover": list(range(25, 97)) + list(range(125, 197)) + [248],
            "Sparse Vegetation": list(range(102, 119)),
            "Dense Short Vegetation": list(range(119, 125)),
            "Crop Land": list(range(244, 250)),
            "Urban Area": list(range(250, 254)),
            "Salt Pan": list(range(100, 102)),
            "Ocean": list(range(208, 211))
        }

    def _setup_logger(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('processing.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)

    def _configure_gdal(self):
        gdal.UseExceptions()
        gdal.SetConfigOption('GDAL_CACHEMAX', '8192')
        gdal.SetConfigOption('VSI_CACHE', 'TRUE')
        gdal.SetConfigOption('VSI_CACHE_SIZE', '1000000')
        gdal.SetConfigOption('COMPRESS_OVERVIEW', 'LZW')
        gdal.SetConfigOption('NUM_THREADS', 'ALL_CPUS')

    def get_umd_class_name(self, pixel_value):
        for class_name, values in self.UMD_CLASSES.items():
            if pixel_value in values:
                return class_name
        return f"Other(UMD_{pixel_value})"

    def align_tiff(self, reference_path, target_path, output_path):
        try:
            # Open reference dataset and get info
            reference_ds = gdal.Open(reference_path)
            if reference_ds is None:
                raise RuntimeError(f"Could not open reference file: {reference_path}")
            
            # Get reference specs
            ref_transform = reference_ds.GetGeoTransform()
            ref_projection = reference_ds.GetProjection()
            ref_width = reference_ds.RasterXSize
            ref_height = reference_ds.RasterYSize
            
            # Calculate bounds
            utm_bounds = (
                ref_transform[0],
                ref_transform[3] + ref_transform[5] * ref_height,
                ref_transform[0] + ref_transform[1] * ref_width,
                ref_transform[3]
            )
            
            # Create warp options
            warp_options = gdal.WarpOptions(
                format='GTiff',
                dstSRS=ref_projection,
                width=ref_width,
                height=ref_height,
                outputBounds=utm_bounds,
                resampleAlg=gdal.GRA_NearestNeighbour,
                multithread=True,
                creationOptions=[
                    'COMPRESS=LZW',
                    'PREDICTOR=2',
                    'BIGTIFF=YES',
                    'TILED=YES',
                    'BLOCKXSIZE=512',
                    'BLOCKYSIZE=512'
                ]
            )
            
            # Perform warping
            self.logger.info(f"Warping {target_path} to match {reference_path}...")
            aligned_ds = gdal.Warp(output_path, target_path, options=warp_options)
            
            if aligned_ds is None:
                raise RuntimeError("Warping failed")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Error aligning TIFF files: {e}")
            return None

    


    def process_tile(self, tile_info):
        """Process tile with proper projection and alignment order"""
        country, tile_name = tile_info
        base_umd_dir = self.base_umd_dir
        base_riparian_dir = self.base_riparian_dir
        temp_dir = self.temp_dir
        
        try:
            rip_tiff_path = os.path.join(base_riparian_dir, country, tile_name)
            umd_tiff_path = os.path.join(base_umd_dir, country, tile_name)
            
            if not os.path.exists(umd_tiff_path):
                return None
                
            # Step 1: First align UMD to riparian in geographic coordinates
            temp_umd_aligned = os.path.join(temp_dir, f"aligned_{tile_name}")
            aligned_path = self.align_tiff(rip_tiff_path, umd_tiff_path, temp_umd_aligned)
            if not aligned_path:
                return None
            
            # Step 2: Get bounds from riparian for projection
            rip_ds = gdal.Open(rip_tiff_path, gdal.GA_ReadOnly)
            gt = rip_ds.GetGeoTransform()
            minx = gt[0]
            maxy = gt[3]
            maxx = gt[0] + gt[1] * rip_ds.RasterXSize
            miny = gt[3] + gt[5] * rip_ds.RasterYSize
            bounds = (minx, miny, maxx, maxy)
            
            # Get appropriate projection
            proj_crs = get_equal_area_projection(bounds)
            dst_wkt = get_projection_wkt(proj_crs)
            
            # Step 3: Project both files to metric
            warp_options = gdal.WarpOptions(
                format='GTiff',
                srcSRS='EPSG:4326',
                dstSRS=dst_wkt,
                xRes=30,
                yRes=30,
                resampleAlg=gdal.GRA_NearestNeighbour,
                multithread=True,
                warpMemoryLimit=1024,
                creationOptions=['COMPRESS=LZW', 'PREDICTOR=2', 'BIGTIFF=YES']
            )
            
            temp_rip_proj = os.path.join(temp_dir, f"rip_proj_{tile_name}")
            temp_umd_proj = os.path.join(temp_dir, f"umd_proj_{tile_name}")
            
            rip_warped = gdal.Warp(temp_rip_proj, rip_tiff_path, options=warp_options)
            umd_warped = gdal.Warp(temp_umd_proj, aligned_path, options=warp_options)
            
            if rip_warped is None or umd_warped is None:
                raise RuntimeError("Failed to project one or both files")
            
            # Get pixel area in square meters
            gt_proj = rip_warped.GetGeoTransform()
            pixel_area = abs(gt_proj[1] * gt_proj[5])
            
            # Process data
            results = defaultdict(lambda: defaultdict(int))
            
            rip_band = rip_warped.GetRasterBand(1)
            umd_band = umd_warped.GetRasterBand(1)
            
            chunk_size = 2048
            for y in range(0, rip_warped.RasterYSize, chunk_size):
                rows = min(chunk_size, rip_warped.RasterYSize - y)
                for x in range(0, rip_warped.RasterXSize, chunk_size):
                    cols = min(chunk_size, rip_warped.RasterXSize - x)
                    
                    rip_data = rip_band.ReadAsArray(x, y, cols, rows)
                    umd_data = umd_band.ReadAsArray(x, y, cols, rows)
                    
                    for rip_class in [4]:
                        rip_mask = rip_data == rip_class
                        if not rip_mask.any():
                            continue
                        
                        pixel_count = np.sum(rip_mask)
                        results[rip_class]['total'] += pixel_count * pixel_area / 10000  # Convert to hectares
                        
                        umd_values = umd_data[rip_mask]
                        unique_values, counts = np.unique(umd_values, return_counts=True)
                        
                        for umd_val, count in zip(unique_values, counts):
                            umd_class = self.get_umd_class_name(umd_val)
                            results[rip_class][umd_class] += count * pixel_area / 10000  # Convert to hectares
                    
                    del rip_data, umd_data
            
            # Clean up
            rip_band = None
            umd_band = None
            rip_warped = None
            umd_warped = None
            
            # Remove temporary files
            for temp_file in [temp_umd_aligned, temp_rip_proj, temp_umd_proj]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            return tile_name, results
            
        except Exception as e:
            self.logger.error(f"Error processing tile {tile_name}: {e}")
            return None

    def _check_existing_processed_files(self, country):
        """
        Check if processed files for a country already exist.
        
        Returns:
        - None if no existing files found
        - Dictionary of existing file paths if found
        """
        temp_dir = self.temp_dir
        existing_files = {
            'aligned_tiles': [],
            'projected_rip_tiles': [],
            'projected_umd_tiles': []
        }
        
        # Check for previously aligned tiles
        aligned_pattern = os.path.join(temp_dir, f"aligned_*{country}*.tif")
        existing_files['aligned_tiles'] = glob.glob(aligned_pattern)
        
        # Check for previously projected riparian tiles
        rip_proj_pattern = os.path.join(temp_dir, f"rip_proj_*{country}*.tif")
        existing_files['projected_rip_tiles'] = glob.glob(rip_proj_pattern)
        
        # Check for previously projected UMD tiles
        umd_proj_pattern = os.path.join(temp_dir, f"umd_proj_*{country}*.tif")
        existing_files['projected_umd_tiles'] = glob.glob(umd_proj_pattern)
        
        # If no existing files found, return None
        if not any(existing_files.values()):
            return None
        
        return existing_files

    def process_single_country(self, country):
        try:
            output_dir = self.output_dir
            temp_dir = self.temp_dir
    
            # Step 1: Check if CSV exists
            csv_path = os.path.join(output_dir, f"{country}_analysis.csv")
            if os.path.exists(csv_path):
                self.logger.info(f"CSV already exists for {country}, skipping...")
                return "skipped"
    
            # Check for existing processed files
            existing_files = self._check_existing_processed_files(country)
            if existing_files:
                self.logger.info(f"Found existing processed files for {country}")
                for file_type, files in existing_files.items():
                    self.logger.info(f"{file_type}: {len(files)} files")
    
            self.logger.info(f"Starting to process country: {country}")
            riparian_dir = os.path.join(self.base_riparian_dir, country)
    
            if not os.path.isdir(riparian_dir):
                self.logger.warning(f"Directory not found for {country}")
                return "failed"
    
            # Get list of all TIFF files for this country
            riparian_tiffs = sorted([f for f in os.listdir(riparian_dir) if f.endswith('.tif')])
            if not riparian_tiffs:
                self.logger.warning(f"No TIFF files found for {country}")
                return "failed"
    
            # Initialize results storage
            results = defaultdict(lambda: defaultdict(int))
            processed_tiles = []
    
            total_tiles = len(riparian_tiffs)
            self.logger.info(f"Found {total_tiles} tiles for {country}")
    
            # Process each tile
            for i, tile in enumerate(riparian_tiffs, 1):
                self.logger.info(f"Processing tile {i}/{total_tiles} for {country}: {tile}")
                try:
                    # If existing files exist, check for the specific tile
                    skip_processing = False
                    if existing_files:
                        # Check if corresponding files for this tile exist
                        for file_type, files in existing_files.items():
                            if any(tile in f for f in files):
                                skip_processing = True
                                break
                    
                    if skip_processing:
                        self.logger.info(f"Skipping tile {tile} as processed files already exist")
                        continue
                    
                    # Process the tile directly
                    result = self.process_tile((country, tile))
                    if result:
                        tile_name, tile_results = result
                        processed_tiles.append(tile_name)
                        
                        # Aggregate results
                        for rip_class, umd_data in tile_results.items():
                            results[rip_class]['total'] += umd_data['total']
                            for umd_class, count in umd_data.items():
                                if umd_class != 'total':
                                    results[rip_class][umd_class] += count
                        
                        self.logger.info(f"Successfully processed tile {tile} for {country}")
                    else:
                        self.logger.warning(f"Failed to process tile {tile} for {country}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing tile {tile} for {country}: {e}")
                    continue
    
            # Write results if any tiles were processed
            if processed_tiles:
                self.logger.info(f"Writing results for {country}. Processed {len(processed_tiles)}/{total_tiles} tiles")
                self._write_results_to_csv(results, country, processed_tiles)
                return "success"
            else:
                self.logger.warning(f"No tiles were successfully processed for {country}")
                return "failed"
                
        except Exception as e:
            self.logger.error(f"Error processing country {country}: {e}")
            return "failed"

    def _write_results_to_csv(self, results, country, processed_tiles):
        output_file = os.path.join(self.output_dir, f"{country}_analysis.csv")
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Country', 'Processed_Tiles', country])
            writer.writerow(['Tiles Processed', len(processed_tiles)])
            writer.writerow(['Tiles', ', '.join(processed_tiles)])
            writer.writerow([])
            writer.writerow(['Riparian_Class', 'UMD_Class', 'Pixel_Count', 'Percentage'])
            
            for rip_class in [4]:
                total = results[rip_class]['total']
                writer.writerow([rip_class, 'Total', total, '100%'])
                
                sorted_items = sorted(
                    [(k, v) for k, v in results[rip_class].items() if k != 'total'],
                    key=lambda x: x[1],
                    reverse=True
                )
                
                for umd_class, count in sorted_items:
                    percentage = (count / total * 100) if total > 0 else 0
                    writer.writerow([
                        rip_class,
                        umd_class,
                        count,
                        f"{percentage:.2f}%"
                    ])
                writer.writerow([])

    def process_all(self, base_umd_dir, base_riparian_dir, output_dir, temp_dir):
        # Store paths as instance variables
        self.base_umd_dir = base_umd_dir
        self.base_riparian_dir = base_riparian_dir
        self.output_dir = output_dir
        self.temp_dir = temp_dir
        
        # Create directories
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)
        
        # Define specific countries to process
        countries = [
            "Australia",  # You can modify this list with your desired countries

        ]
        
        # Process countries
        results = {'success': [], 'failed': [], 'skipped': []}
        
        # Use Pool for country-level parallelization
        with mp.Pool(processes=min(len(countries), mp.cpu_count())) as pool:
            for country, status in zip(countries, pool.imap(self.process_single_country, countries)):
                results[status].append(country)
                self.logger.info(f"Completed {country} with status: {status}")
        
        return results
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
def get_projection_wkt(proj_crs):
        """Convert projection identifier to WKT format"""
        out_srs = osr.SpatialReference()
        
        if proj_crs.startswith('EPSG:'):
            if proj_crs == "EPSG:5070":  # USA Contiguous Albers Equal Area Conic
                usa_wkt = '''PROJCS["NAD83 / Conus Albers",
                    GEOGCS["NAD83",
                        DATUM["North_American_Datum_1983",
                            SPHEROID["GRS 1980",6378137,298.257222101,
                                AUTHORITY["EPSG","7019"]],
                            AUTHORITY["EPSG","6269"]],
                        PRIMEM["Greenwich",0,
                            AUTHORITY["EPSG","8901"]],
                        UNIT["degree",0.0174532925199433,
                            AUTHORITY["EPSG","9122"]],
                        AUTHORITY["EPSG","4269"]],
                    PROJECTION["Albers_Conic_Equal_Area"],
                    PARAMETER["latitude_of_center",23],
                    PARAMETER["longitude_of_center",-96],
                    PARAMETER["standard_parallel_1",29.5],
                    PARAMETER["standard_parallel_2",45.5],
                    PARAMETER["false_easting",0],
                    PARAMETER["false_northing",0],
                    UNIT["metre",1,
                        AUTHORITY["EPSG","9001"]],
                    AXIS["X",EAST],
                    AXIS["Y",NORTH],
                    AUTHORITY["EPSG","5070"]]'''
                out_srs.ImportFromWkt(usa_wkt)
            elif proj_crs == "EPSG:3978":  # Canada Atlas Lambert
                canada_wkt = '''PROJCS["NAD83 / Canada Atlas Lambert",
                    GEOGCS["NAD83",
                        DATUM["North_American_Datum_1983",
                            SPHEROID["GRS 1980",6378137,298.257222101]],
                        PRIMEM["Greenwich",0],
                        UNIT["degree",0.0174532925199433]],
                    PROJECTION["Lambert_Conformal_Conic_2SP"],
                    PARAMETER["standard_parallel_1",49],
                    PARAMETER["standard_parallel_2",77],
                    PARAMETER["latitude_of_origin",49],
                    PARAMETER["central_meridian",-95],
                    PARAMETER["false_easting",0],
                    PARAMETER["false_northing",0],
                    UNIT["metre",1]]'''
                out_srs.ImportFromWkt(canada_wkt)
            elif proj_crs == "EPSG:2193":  # New Zealand Transverse Mercator 2000
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

# Usage example
def main():
    processor = TiffProcessor()
    base_umd_dir = "UMD_results"
    base_riparian_dir = "result_max"
    output_dir = "csv_results3"
    temp_dir = "temp_aligned_tiffs3"
    
    results = processor.process_all(base_umd_dir, base_riparian_dir, output_dir, temp_dir)
    print("Processing completed with results:", results)

if __name__ == "__main__":
    main()