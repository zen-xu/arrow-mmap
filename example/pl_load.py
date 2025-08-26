import polars as pl

# display all rows
pl.Config.set_tbl_rows(-1)

df = pl.read_ipc("stream.arrow")
print(df)
