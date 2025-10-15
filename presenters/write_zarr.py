import zarr
import matplotlib.image as mpimg


# TODO set zarr concurrency to 1?


def write_image(img_path: str, store: zarr.storage.ObjectStore):
    img_arr = mpimg.imread(img_path)

    grp = zarr.open_group(store=store, mode='w')

    # write each chunk in a random order
    for chunk_slice in generate_chunk_slices(shape=img_arr.shape, n_chunks=25):
        # TODO randomly reorder the chunk slices

        # TODO use LatencyStore
        # TODO write one chunk
        ...


def generate_chunk_slices(shape: tuple[int], n_chunks: int):
    ...


store = zarr.storage.LocalStore(..., read_only=False)
#store = zarr.storage.ObjectStore("s3://bucket/mystery.zarr", read_only=False)


while True:
    # write alive image
    write_image("alive.png", store)

    # write dead image
    write_image("dead.png", store)
