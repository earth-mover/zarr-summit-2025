import zarr
from zarr.testing.store import LatencyStore
import matplotlib.image as mpimg
import numpy as np
import random
from typing import Iterator
from pathlib import Path


# Set zarr async concurrency to 1 to ensure sequential writes
zarr.config.set({'async.concurrency': 1})


def generate_chunk_slices(shape: tuple[int, int, int], n_chunks: int) -> Iterator[tuple[slice, ...]]:
    """
    Generate chunk slices for a 3D array (height, width, channels).
    Divides the image into n_chunks x n_chunks grid (ignoring the channel dimension).
    Returns chunk slices in random order.
    """
    height, width, channels = shape
    chunk_height = height // n_chunks
    chunk_width = width // n_chunks

    slices = []
    for i in range(n_chunks):
        for j in range(n_chunks):
            h_start = i * chunk_height
            h_end = (i + 1) * chunk_height if i < n_chunks - 1 else height
            w_start = j * chunk_width
            w_end = (j + 1) * chunk_width if j < n_chunks - 1 else width

            slices.append((slice(h_start, h_end), slice(w_start, w_end), slice(None)))

    # Randomly shuffle the chunk order to simulate non-atomic writes
    random.shuffle(slices)
    return iter(slices)


def write_image(img_path: str, store: zarr.storage.Store):
    """Write an image to zarr store chunk by chunk in random order."""
    img_arr = mpimg.imread(img_path)

    # Ensure img_arr is float32 and has shape (height, width, channels)
    if img_arr.dtype != np.float32:
        img_arr = img_arr.astype(np.float32)

    # Create array if it doesn't exist, or open existing
    root = zarr.open_group(store=store, mode='a')

    # Calculate chunk size based on n_chunks
    n_chunks = 5
    chunk_shape = (img_arr.shape[0] // n_chunks, img_arr.shape[1] // n_chunks, img_arr.shape[2])

    if 'image' not in root:
        arr = root.create_array(
            'image',
            shape=img_arr.shape,
            chunks=chunk_shape,
            dtype=np.float32,
            overwrite=True
        )
    else:
        arr = root['image']

    # Write each chunk in random order with latency
    for chunk_slice in generate_chunk_slices(shape=img_arr.shape, n_chunks=n_chunks):
        arr[chunk_slice] = img_arr[chunk_slice]
        print(f"Wrote chunk {chunk_slice}")


# Configure store with latency wrapper
store_path = Path("./zarr_store")
store_path.mkdir(exist_ok=True)
base_store = zarr.storage.LocalStore(store_path, read_only=False)
store = LatencyStore(base_store, delay=0.2)  # 200ms delay per write

# For S3:
# base_store = zarr.storage.ObjectStore("s3://bucket/mystery.zarr", read_only=False)
# store = LatencyStore(base_store, delay=0.2)


if __name__ == "__main__":
    print("Starting zarr writer (no ACID transactions)...")
    print("Writing images continuously - readers may see partial updates!")

    while True:
        # write alive image
        print("\n=== Writing 'alive.png' ===")
        write_image("alive.png", store)

        # write dead image
        print("\n=== Writing 'dead.png' ===")
        write_image("dead.png", store)
