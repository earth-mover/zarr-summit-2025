# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "icechunk",
# ]
# ///
import matplotlib.pyplot as plt
import zarr
from pathlib import Path
from icechunk import Repository, local_filesystem_storage


# Open the local Icechunk repository
repo_path = Path("../presenters/test_icechunk_store")
storage = local_filesystem_storage(str(repo_path.absolute()))
repo = Repository.open_or_create(storage)

print(f"Reading from Icechunk store at: {repo_path.absolute()}")

# Start a read-only session
session = repo.readonly_session("main")
store = session.store

# Get all the zarr array values
root = zarr.open_group(store=store, mode='r')
arr = root['image']
img_data = arr[:]

# Plot the data
plt.imshow(img_data)
plt.title("Icechunk Store (ACID) - Always complete snapshots!")
plt.axis('off')
plt.show()
