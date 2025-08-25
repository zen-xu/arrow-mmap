import nanoarrow.ipc as na_ipc
import nanoarrow as na
import pyarrow as pa

with na_ipc.InputStream.from_path("stream.arrow") as input_stream:
    cstream = na.c_array_stream(input_stream)
    batch = pa.RecordBatchReader._import_from_c(cstream._addr()).read_next_batch()
    df = batch.to_pandas()

print(df)
