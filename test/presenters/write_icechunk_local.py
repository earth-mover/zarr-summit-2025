# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "numpy",
#   "icechunk",
# ]
# ///
import zarr
import matplotlib.image as mpimg
import numpy as np
import random
from typing import Iterator
from pathlib import Path
from icechunk import Repository, local_filesystem_storage


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

    # Randomly shuffle the chunk order (same as zarr version)
    random.shuffle(slices)
    return iter(slices)


def write_image(img_path: str, store):
    """Write an image to zarr store chunk by chunk in random order."""
    img_arr = mpimg.imread(img_path)

    # Ensure img_arr is float32 and has shape (height, width, channels)
    if img_arr.dtype != np.float32:
        img_arr = img_arr.astype(np.float32)

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

    # Write each chunk in random order with latency
    for chunk_slice in generate_chunk_slices(shape=img_arr.shape, n_chunks=n_chunks):
        arr[chunk_slice] = img_arr[chunk_slice]
        print(f"Wrote chunk {chunk_slice}")


if __name__ == "__main__":
    print("Starting local Icechunk writer (with ACID transactions)...")
    print("Writing images continuously - readers will only see complete committed snapshots!")

    # Create local filesystem storage for Icechunk
    repo_path = Path("./test_icechunk_store")
    repo_path.mkdir(exist_ok=True)

    storage = local_filesystem_storage(str(repo_path.absolute()))
    repo = Repository.open_or_create(storage)

    # reset repo to starting state, in case this script isn't running from a clean state
    repo.reset_branch(branch="main", snapshot_id="1CECHNKREP0F1RSTCMT0")

    print(f"Store location: {repo_path.absolute()}\n")

    while True:
        # Create a writable session
        session = repo.writable_session("main")

        # Write alive image
        print("\n=== Writing 'alive.png' ===")
        write_image("../../presenters/images/alive.png", session.store)

        # Commit the transaction - this makes all writes atomic!
        snapshot_id = session.commit("Updated image from alive.png")
        print(f"Committed transaction: {snapshot_id}")

        # Create a new writable session
        session = repo.writable_session("main")

        # Write dead image
        print("\n=== Writing 'dead.png' ===")
        write_image("../../presenters/images/dead.png", session.store)

        # Commit the transaction
        snapshot_id = session.commit("Updated image from dead.png")
        print(f"Committed transaction: {snapshot_id}")
