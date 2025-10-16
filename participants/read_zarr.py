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
root = zarr.open_group(store="s3://zarr-summit-italy-public/zarr3", mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.axis('off')
plt.show()
