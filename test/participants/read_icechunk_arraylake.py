# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "icechunk",
#   "arraylake",
# ]
# ///
import matplotlib.pyplot as plt
import zarr
import time
from arraylake import Client


# Connect to Arraylake repository
client = Client()
repo = client.get_repo("earthmover-demos/zarr-summit-2025")

print(f"Reading from Arraylake repository: earthmover-demos/zarr-summit-2025")
print("Press Ctrl+C to exit\n")

while True:
    try:
        # Start a read-only session
        session = repo.readonly_session("main")
        store = session.store

        # Get all the zarr array values
        root = zarr.open_group(store=store, mode='r')
        arr = root['image']
        img_data = arr[:]

        # Plot the data
        plt.clf()
        plt.imshow(img_data)
        plt.title("Icechunk/Arraylake Store (ACID) - Always complete snapshots!")
        plt.axis('off')
        plt.pause(0.5)

        time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nExiting...")
        break
    except Exception as e:
        print(f"Error reading data: {e}")
        time.sleep(1)
