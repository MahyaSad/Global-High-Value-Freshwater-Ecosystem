import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from scipy.ndimage import binary_dilation
import matplotlib.pyplot as plt
from rasterio.windows import from_bounds, Window
import geopandas as gpd
from dask.distributed import Client, LocalCluster
from tqdm import tqdm
import os
import numpy as np
import os


def get_reference_extent(filepath, bounds):
    """Get reference extent and transform from stream dataset."""
    with rasterio.open(filepath) as src:
        # Calculate window for the bounds
        window = from_bounds(*bounds, src.transform)
        
        # Get the exact dimensions
        width = int(window.width)
        height = int(window.height)
        
        # Calculate new transform for the window
        window_transform = src.window_transform(window)
        
        return width, height, window_transform, src.crs

def read_and_mask_dataset(filepath, bounds, ref_width, ref_height, ref_transform):
    """Read dataset aligned to reference extent."""
    with rasterio.open(filepath) as src:
        # Create destination array
        dest = np.zeros((ref_height, ref_width), dtype=src.dtypes[0])
        
        # Reproject data to match reference
        reproject(
            source=rasterio.band(src, 1),
            destination=dest,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=ref_transform,
            dst_crs=src.crs,
            resampling=Resampling.nearest
        )
        
        return dest
def extract_flood_buffer(floodplain_tiff, bounds, threshold_pixels, buffer_distance_pixels):
    """
    Extract a flood buffer for narrow sections of a floodplain raster.

    Args:
        floodplain_tiff (str): Path to the floodplain raster file.
        bounds (tuple): Bounding box (min_lon, min_lat, max_lon, max_lat).
        threshold_pixels (int): Maximum width for narrow floodplain sections.
        buffer_distance_pixels (int): Distance for buffer creation in pixels.

    Returns:
        ndarray: A binary mask representing the flood buffer.
    """
    from scipy.ndimage import binary_dilation

    # Step 1: Extract the floodplain subset
    floodplain, transform, crs = extract_raster_subset(floodplain_tiff, bounds)

    # Step 2: Convert floodplain raster to binary mask
    binary_mask = (floodplain == 1).astype(np.uint8)

    # Step 3: Initialize buffer mask
    buffer_mask = np.zeros_like(binary_mask, dtype=np.uint8)

    # Step 4: Check horizontal widths row by row
    for row_idx in range(binary_mask.shape[0]):
        row_data = binary_mask[row_idx, :]
        # Identify clusters of floodplain pixels in the row
        clusters = np.where(np.diff(np.concatenate(([0], row_data, [0]))) != 0)[0].reshape(-1, 2)

        for cluster in clusters:
            start_idx, end_idx = cluster[0], cluster[1] - 1
            width_pixels = end_idx - start_idx + 1
            if width_pixels <= threshold_pixels:
                # Create a buffer only for narrow horizontal clusters
                narrow_cluster_mask = np.zeros_like(row_data, dtype=np.uint8)
                narrow_cluster_mask[start_idx:end_idx + 1] = 1
                row_buffer_mask = create_edge_buffer_1d(narrow_cluster_mask, buffer_distance_pixels)
                buffer_mask[row_idx, :] |= row_buffer_mask

    # Step 5: Check vertical widths column by column
    for col_idx in range(binary_mask.shape[1]):
        col_data = binary_mask[:, col_idx]
        # Identify clusters of floodplain pixels in the column
        clusters = np.where(np.diff(np.concatenate(([0], col_data, [0]))) != 0)[0].reshape(-1, 2)

        for cluster in clusters:
            start_idx, end_idx = cluster[0], cluster[1] - 1
            width_pixels = end_idx - start_idx + 1
            if width_pixels <= threshold_pixels:
                # Create a buffer only for narrow vertical clusters
                narrow_cluster_mask = np.zeros_like(col_data, dtype=np.uint8)
                narrow_cluster_mask[start_idx:end_idx + 1] = 1
                col_buffer_mask = create_edge_buffer_1d(narrow_cluster_mask, buffer_distance_pixels)
                buffer_mask[:, col_idx] |= col_buffer_mask

    return buffer_mask


def create_stream_buffer(data, buffer_distance_pixels):
    """
    Create buffer by adding pixels to increase stream width from edges
    """
    binary = data > 0
    buffer_size = int(buffer_distance_pixels)
    
    # Create cross-shaped structuring element to expand in all directions
    structure = np.array([[1, 1, 1],
                         [1, 1, 1],
                         [1, 1, 1]])
    
    # Create buffer by dilating once
    dilated = binary_dilation(binary, structure)
    stream_buffer = dilated & ~binary
    
    return stream_buffer   
def extract_raster_subset(raster_path, bounds):
    """
    Extract a subset of the raster using specified bounds.
    """
    with rasterio.open(raster_path) as src:
        window = from_bounds(*bounds, src.transform)
        subset = src.read(1, window=window)
        transform = src.window_transform(window)
    return subset, transform, src.crs
def create_edge_buffer_1d(binary_mask, buffer_distance_pixels):
    """
    Create a buffer only around the outer edge of features in 1D.
    """
    structure = np.ones(buffer_distance_pixels * 2 + 1, dtype=bool)  # Structuring element for 1D dilation
    dilated = binary_dilation(binary_mask, structure=structure)
    buffer_mask = dilated & ~binary_mask  # Buffer is the dilated area minus the original floodplain
    return buffer_mask

def create_edge_buffer(data, buffer_distance_pixels):
    """Create buffer only from the outer edge of features."""
    binary = data > 0
    buffer_size = int(buffer_distance_pixels)
    structure = np.ones((2 * buffer_size + 1, 2 * buffer_size + 1))
    
    # Create the outer buffer only
    dilated = binary_dilation(binary, structure)
    outer_buffer = dilated & ~binary
    
    # Fill internal holes/gaps
    filled_binary = binary.copy()
    from scipy.ndimage import binary_fill_holes
    filled_binary = binary_fill_holes(filled_binary)
    
    # Only keep buffer around the outer edge
    outer_edge_buffer = outer_buffer & ~binary_fill_holes(binary)
    
    return outer_edge_buffer

def get_floodplain_width(floodplain_mask):
    """
    Calculate floodplain width along the x-axis (horizontal direction) for a given floodplain mask.
    This computes the number of contiguous floodplain pixels across each row.

    Args:
        floodplain_mask (ndarray): A binary mask where floodplain pixels are True.

    Returns:
        ndarray: An array of maximum floodplain widths per row (in pixels).
    """
    import numpy as np

    # Initialize an array to store the maximum width for each row
    floodplain_widths = np.zeros(floodplain_mask.shape[0], dtype=int)

    # Iterate through each row in the floodplain mask
    for row_idx, row in enumerate(floodplain_mask):
        # Find all contiguous floodplain regions in the row
        contiguous_regions = np.diff(np.where(np.concatenate(([0], row, [0])))[0]) - 1
        
        # Get the maximum width of contiguous regions in this row
        if contiguous_regions.size > 0:
            floodplain_widths[row_idx] = np.max(contiguous_regions)

    return floodplain_widths

def debug(message):
    print(f"[DEBUG] {message}")

def process_window(bounds, input_files, ref_transform):
    """Process a single window with updated floodplain width and overlap handling."""
    try:
        # Get reference dimensions
        ref_width, ref_height, window_transform, ref_crs = get_reference_extent(
            input_files['stream'], bounds
        )

        if ref_width == 0 or ref_height == 0:
            return None

        # Read and process UMD data
        umd_data = read_and_mask_dataset(input_files['umd'], bounds, ref_width, ref_height, window_transform)
        umd_nodata_mask = (umd_data == 255)  # Assuming 255 is No Data in UMD
        umd_exclude_mask = np.isin(umd_data, [0, 1, 241, 242, 243,254,100,101])  # Desert, snow/ice classes
        umd_mask = ~umd_nodata_mask & ~umd_exclude_mask  # Valid UMD regions, excluding No Data and specified classes

        # Process datasets with UMD masking
        datasets = {}
        for name, filepath in input_files.items():
            
            data = read_and_mask_dataset(filepath, bounds, ref_width, ref_height, window_transform)

            if name == 'umd':
                mask = np.isin(data, [207, 208, 209, 210, 211])
                datasets['surface_water'] = mask.astype(bool)
                water_buffer = create_edge_buffer(datasets['surface_water'], 2)
                inundated_mask=(data >= 102) & (data <= 196)
                datasets['inundated_wetland'] = (inundated_mask ).astype(bool) 

            elif name == 'stream': 
                first_stream_mask = (data >= 1) & (data <= 3)
                high_stream_mask = (data >= 4) & (data <= 11)
                datasets['low_stream'] = (first_stream_mask & umd_mask).astype(bool)
                datasets['high_stream'] = (high_stream_mask & umd_mask).astype(bool)
                high_stream_buffer = create_stream_buffer(datasets['high_stream'], 1)
            elif name == 'floodplain':
                mask = (data == 1) 
                datasets['floodplain'] = (mask & umd_mask).astype(bool)
            elif name == 'subcatch':
                mask = (data == 2)
                datasets['subcatch'] = (mask & umd_mask).astype(bool)
            
        
        
        # Remove overlap of high order stream and its buffer with surface water and its buffer
        overlap_surface = (datasets['high_stream'])  & (
                    datasets['surface_water'] | water_buffer
                )
        datasets['high_stream'][overlap_surface] = False
                           
        overlap_surface = high_stream_buffer & (
                    datasets['surface_water'] | water_buffer
                )
        high_stream_buffer[overlap_surface] = False
        
        overlap_surface = (datasets['low_stream'])  & (
                    datasets['surface_water'] | water_buffer
                )
        datasets['low_stream'][overlap_surface] = False
        
        stream_buffer=high_stream_buffer|datasets['low_stream']|datasets['high_stream']

        
        # Create result array
        result = np.zeros((ref_height, ref_width), dtype=np.uint8)

        overlap_inundated = datasets['inundated_wetland'] & (
            datasets['surface_water']|stream_buffer|water_buffer
        )
        datasets['inundated_wetland'][overlap_inundated] = False
        wetland_buffer = create_stream_buffer(datasets['inundated_wetland'], 2)
        
        
        overlap_gw = datasets['gw_wetland'] & (
            datasets['surface_water'] | datasets['floodplain'] | datasets['inundated_wetland'] | stream_buffer|water_buffer|wetland_buffer|datasets['subcatch']
        )
        datasets['gw_wetland'][overlap_gw] = False

        # Subcatchment overlaps
        overlap_subcatch = datasets['subcatch'] & (
            datasets['surface_water'] |datasets['inundated_wetland']|stream_buffer
        )
        datasets['subcatch'][overlap_subcatch] = False
        
        
        overlap_buffer = wetland_buffer & (
            datasets['subcatch']|datasets['inundated_wetland']|datasets['surface_water']|stream_buffer|water_buffer
        )
        wetland_buffer[overlap_buffer] = False
        
        overlap_buffer = water_buffer & (
            datasets['subcatch']|datasets['surface_water']
        )
        water_buffer[overlap_buffer] = False
        
        
        
        
        # Create result array
        result = np.zeros((ref_height, ref_width), dtype=np.uint8)

        # Apply classification rules
        result[datasets['surface_water']] = 1

        # Inundated wetland
        temp_mask = datasets['inundated_wetland'] 
        result[temp_mask] = 2


        # Subcatchment
        temp_mask = datasets['subcatch'] 
        result[temp_mask] = 3

        
        # Include water buffer
        result[water_buffer] = 5
        result[high_stream_buffer] = 5
        result[datasets['low_stream']] = 4
        result[datasets['high_stream']] = 5
        
        #result[wetland_buffer]=8
        
        
        # Calculate pixel coordinates
        with rasterio.open(input_files['stream']) as src:
            row_start, col_start = src.index(bounds[0], bounds[3])
        
        # Create mask for the specified region
        rows, cols = result.shape
        y_coords = np.linspace(bounds[3], bounds[1], rows)
        x_coords = np.linspace(bounds[0], bounds[2], cols)
        X, Y = np.meshgrid(x_coords, y_coords)
        mask_region = (Y >= 24.8) & (Y <= 29.3) & (X >= -118.5) & (X <= -115.6)
        result[mask_region] = 0
        
        return bounds, result, row_start, col_start
        
    except Exception as e:
        print(f"Error processing window {bounds}: {str(e)}")
        return None

def process_by_tiles(input_files, output_path, window_size=5):
    """Process area by tiles using parallel window processing."""
    # Get bounds from stream order file
    with rasterio.open(input_files['stream']) as src:
        ref_transform = src.transform
        ref_profile = src.profile.copy()
        bounds = src.bounds
        
    # Use bounds from the stream file
    print(f"Processing area with bounds from stream file: {bounds}")
    
    # Create windows
    windows = []
    for x in np.arange(bounds.left, bounds.right, window_size):
        for y in np.arange(bounds.bottom, bounds.top, window_size):
            window_bounds = (
                x,
                y,
                min(x + window_size, bounds.right),
                min(y + window_size, bounds.top)
            )
            windows.append(window_bounds)
    
    print(f"Created {len(windows)} windows of size {window_size} degrees")
    
    # Rest of your existing code remains the same
    cluster = LocalCluster(
        n_workers=24,
        threads_per_worker=2,
        memory_limit="16GB",
        local_directory="/tmp/dask-temp"
    )
    client = Client(cluster)
    
    try:
        # Process windows in parallel
        print("\nProcessing windows...")
        futures = []
        for window_bounds in windows:
            future = client.submit(process_window, window_bounds, input_files, ref_transform)
            futures.append(future)

        
        # Collect results
        results = []
        for future in tqdm(futures, desc="Processing windows"):
            result = future.result()
            if result is not None:
                results.append(result)
        
        if not results:
            raise ValueError("No valid results were produced")
        
        # Create output raster
        ref_profile.update({
            'dtype': 'uint8',
            'count': 1,
            'nodata': 0,
            'compress': 'lzw'
        })
        
        # Save the combined result
        with rasterio.open(output_path, 'w', **ref_profile) as dst:
            for bounds, result, row_start, col_start in results:
                try:
                    window = Window(
                        col_off=col_start,
                        row_off=row_start,
                        width=result.shape[1],
                        height=result.shape[0]
                    )
                    
                    if (window.col_off + window.width <= dst.width and 
                        window.row_off + window.height <= dst.height):
                        dst.write(result, 1, window=window)
                    else:
                        print(f"Skipping window at ({row_start}, {col_start}) - out of bounds")
                except Exception as e:
                    print(f"Error writing window at ({row_start}, {col_start}): {str(e)}")
        
        print(f"\nProcessing completed. Output saved to: {output_path}")
        
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    input_files = {
        'floodplain': 'merged_GFP_tile2_30.tif',
        'stream': 'merged_order_tile2_30.tif',
        'subcatch': 'merged_subcatchments_tile2_30.tif',        
        'umd': 'UMD_tile2_30.tif'
    }
    
    process_by_tiles(
        input_files,
        
        "riparian_full_min_tile2.tif",
        window_size=2  # 2-degree windows
    )
    
