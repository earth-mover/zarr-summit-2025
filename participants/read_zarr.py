# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "s3fs",
# ]
# ///
import matplotlib.pyplot as plt
import zarr

# Get all the zarr array values
store = zarr.storage.FsspecStore.from_url("s3://zarr-summit-italy-public/zarr3", storage_options={'anon': True})
root = zarr.open_group(store=store, mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.axis('off')
plt.show()
