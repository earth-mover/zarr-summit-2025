## Instructions for presenters

To run the demo, you need to be continuously running one (or both) write scripts while participants run the corresponding read scripts.

### Writing

For the icechunk/arraylake demo, you can just run (from the same directory as this readme file) `uv run write_icechunk.py`.

For the zarr demo, you need to set AWS credentials for the bucket in your environment, then run `uv run write_zarr.py`.

Both scripts should be able to be resumed from being cancelled.

### Reading

You can use the participants scripts to read once at a time, or use the `read_icechunk/zarr_loop.py` versions of the scripts to keep updating the image every couple of seconds, in case you want to leave it running while you talk.