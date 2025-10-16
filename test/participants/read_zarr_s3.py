# /// script
# dependencies = [
#   "zarr>=3.0.0",
#   "matplotlib",
#   "s3fs",
# ]
# ///
import matplotlib.pyplot as plt
import zarr
import time


# Open the S3 zarr store
s3_path = "s3://zarr-summit-italy-public/zarr3/"
store = zarr.storage.ObjectStore.from_url(s3_path)

print(f"Reading from S3 zarr store at: {s3_path}")
print("Press Ctrl+C to exit\n")

while True:
    try:
        # Get all the zarr array values
        root = zarr.open_group(store=store, mode='r')
        arr = root['image']
        img_data = arr[:]

        # Plot the data
        plt.clf()
        plt.imshow(img_data)
        plt.title("Zarr S3 Store (No ACID) - May show partial updates!")
        plt.axis('off')
        plt.pause(0.5)

        time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nExiting...")
        break
    except Exception as e:
        print(f"Error reading data: {e}")
        time.sleep(1)
