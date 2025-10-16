# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "arraylake",
# ]
# ///
import matplotlib.pyplot as plt
import zarr
import arraylake as al

# Log in to arraylake
client = al.Client()

# Open the Icechunk repo from the arraylake catalog
ic_repo = client.get_repo("earthmover-demos/zarr-summit-2025")

# Start a read-only icechunk session
session = ic_repo.readonly_session("main")

# Get the zarr store
store = session.store

# Get all the zarr array values
root = zarr.open_group(store=store, mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.axis('off')
plt.show()
