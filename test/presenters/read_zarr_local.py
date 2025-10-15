# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
# ]
# ///
import matplotlib.pyplot as plt
import zarr
from pathlib import Path


# Open the local zarr store
store_path = Path("../presenters/test_zarr_store")
store = zarr.storage.LocalStore(store_path, read_only=True)

print(f"Reading from zarr store at: {store_path.absolute()}")

# Get all the zarr array values
root = zarr.open_group(store=store, mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.title("Zarr Store (No ACID) - May show partial updates!")
plt.axis('off')
plt.show()
