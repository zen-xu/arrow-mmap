import pyarrow.ipc as pa_ipc


with open("stream.arrow", "rb") as f:
    df = pa_ipc.open_file(f).read_all().to_pandas()

print(df)
