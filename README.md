# zarr-summit-2025

Welcome to Earthmover's workshop at the Zarr Summit 2025! This repo contains the code for a fun demo of Icechunk and Arraylake.

## "Quantum States" demo

### Instructions for participants

1. Get setup to run the demo locally:
    1. Clone this repository and `cd` to the top-level directory of the cloned repo.
    2. [Install `uv`](https://docs.astral.sh/uv/getting-started/installation/) on your system (`uv` is a fast python package manager).
2. Get setup in the [Earthmover Platform](https://docs.earthmover.io/):
    1. Create a free account by [clicking here](https://app.earthmover.io/login) then clicking `Get Started`.
    2. Log in to ArrayLake by entering `uvx arraylake auth login` in your terminal.
4. Wait until the presenter indicates you should run your script.
5. Run the demo! Try running both of these scripts locally using uv (just copy and paste the uv command into your terminal):
    1. `uv run ./participants/read_zarr.py`
    2. `uv run ./participants/read_icechunk.py`

Run the scripts a few more times each - what do you notice? What do you think is going on?

## Quickstart demo

Run `uv run --with jupyter --with icechunk jupyter lab`
