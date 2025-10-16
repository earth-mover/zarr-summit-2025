# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
# ]
# ///
import matplotlib.pyplot as plt
import zarr

# Open the zarr store
store = zarr.storage.ObjectStore("s3://bucket/mystery.zarr", read_only=True)

# Get all the zarr array values
root = zarr.open_group(store=store, mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.axis('off')
plt.show()
