import nanoarrow as na
import nanoarrow.ipc
import pyarrow as pa

with nanoarrow.ipc.InputStream.from_path("stream.arrow") as input_stream:
    stream = na.c_array_stream(input_stream)
    reader = pa.RecordBatchStreamReader._import_from_c(stream._addr())
    batch =reader.read_next_batch()

print(batch)
