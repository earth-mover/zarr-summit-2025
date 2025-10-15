import matplotlib.pyplot as plt
import xarray as xr
import zarr

# open the zarr store
store = zarr.storage.ObjectStore("s3://bucket/mystery.zarr", read_only=True)

# get all the zarr array values
da = xr.open_zarr(store).load()

# plot the data
img_array = da.plot.imshow(rbg="rgb")
plt.show()
