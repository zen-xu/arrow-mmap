import polars as pl

# display all rows
pl.Config.set_tbl_rows(-1)

df = pl.read_ipc_stream("stream.arrow")
print(df)
