# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "numpy",
#   "arraylake",
# ]
# ///
import zarr
import matplotlib.image as mpimg
import numpy as np
import random
from typing import Iterator
import arraylake as al

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

    # Randomly shuffle the chunk order (same as zarr version)
    random.shuffle(slices)
    return iter(slices)


def write_image_icechunk(img_path: str, session):
    """
    Write an image to Icechunk store chunk by chunk in random order.
    Uses Icechunk session for ACID transactions - all chunks committed atomically.
    """
    img_arr = mpimg.imread(img_path)

    # Ensure img_arr is float32 and has shape (height, width, channels)
    if img_arr.dtype != np.float32:
        img_arr = img_arr.astype(np.float32)

    # Get the store from the session
    store = session.store

    # Create array if it doesn't exist, or open existing
    root = zarr.open_group(store=store, mode='a')

    # Calculate chunk size based on n_chunks
    n_chunks = 10
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

    # Write each chunk in random order
    # All writes happen within the same transaction
    for chunk_slice in generate_chunk_slices(shape=img_arr.shape, n_chunks=n_chunks):
        arr[chunk_slice] = img_arr[chunk_slice]
        print(f"Wrote chunk {chunk_slice}")

    # Commit the transaction - this makes all writes atomic!
    snapshot_id = session.commit(f"Updated image from {img_path}")
    print(f"Committed transaction: {snapshot_id}")
    return snapshot_id


if __name__ == "__main__":
    print("Starting Icechunk writer (with ACID transactions)...")
    print("Writing images continuously - readers will only see complete committed snapshots!")

    # Login to arraylake
    client = al.Client()

    # Get the Icechunk repo from the arraylake catalog
    ic_repo = client.get_repo("earthmover-demos/zarr-summit-2025")

    while True:
        # Create a new writable session for each write cycle
        session = ic_repo.writable_session("main")

        # Write alive image and commit
        print("\n=== Writing array state 1 ===")
        write_image_icechunk("./images/state1.png", session)

        # Create a new session for the next write
        session = ic_repo.writable_session("main")

        # Write dead image and commit
        print("\n=== Writing array state 2 ===")
        write_image_icechunk("./images/state2.png", session)
