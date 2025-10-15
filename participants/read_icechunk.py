import matplotlib.pyplot as plt
import xarray as xr
import arraylake as al

# log in to arraylake
client = al.Client()
client.login()

# open the Icechunk repo from the arraylake catalog
ic_repo = client.get_repo("earthmover-public/zarr-summit")

# start a read-only icechunk session
session = ic_repo.readonly_session("main")

# get the zarr store
session.store

# get all the zarr array values
da = xr.open_zarr(session.store).load()

# plot the data
img_array = da.plot.imshow(rbg="rgb")
plt.show()
