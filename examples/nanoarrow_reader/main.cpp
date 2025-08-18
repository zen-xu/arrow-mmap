#include <nanoarrow/nanoarrow_ipc.hpp>

#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

#define RETURN_IF_STATUS_NOT_OK(s) \
  if (!(s).ok()) {                 \
    return nullptr;                \
  }

const auto kSchema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("salary", arrow::float32())});

const std::shared_ptr<arrow::RecordBatch> create_batch(std::vector<int32_t> ids, std::vector<float> salaries) {
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> salary_array;
  arrow::Int32Builder id_builder;
  arrow::FloatBuilder salary_builder;
  RETURN_IF_STATUS_NOT_OK(id_builder.AppendValues(ids));
  RETURN_IF_STATUS_NOT_OK(salary_builder.AppendValues(salaries));
  RETURN_IF_STATUS_NOT_OK(id_builder.Finish(&id_array));
  RETURN_IF_STATUS_NOT_OK(salary_builder.Finish(&salary_array));
  return arrow::RecordBatch::Make(kSchema, ids.size(), {id_array, salary_array});
}

bool WriteStreamToFile(const std::string& filepath, ArrowArrayStream& stream) {
  // 打开文件
  FILE* file = fopen(filepath.c_str(), "wb");
  if (!file) {
    return false;
  }

  // 创建输出流
  nanoarrow::ipc::UniqueOutputStream output_stream;
  struct ArrowError error;
  if (ArrowIpcOutputStreamInitFile(output_stream.get(), file, true) != NANOARROW_OK) {
    fclose(file);
    return false;
  }

  // 创建 writer
  nanoarrow::ipc::UniqueWriter writer;
  if (ArrowIpcWriterInit(writer.get(), output_stream.get()) != NANOARROW_OK) {
    return false;
  }

  ArrowError error_code;
  if (ArrowIpcWriterWriteArrayStream(writer.get(), &stream, &error_code) != NANOARROW_OK) {
    printf("Error: %s\n", error_code.message);
    return false;
  }

  return true;
}

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  for (auto& field : kSchema->fields()) {
    quill::info(logger, "field.type = {}", field->type()->name());
    quill::info(logger, "field.type_id = {}", static_cast<int>(field->type()->id()));
  }

  auto db = mmap_db::arrow::ArrowDB("db");
  db.create(2, 100, 6, kSchema);
  auto reader = db.reader();

  nanoarrow::UniqueArrayStream array_stream;

  auto writer1 = db.writer(0);
  writer1.write(create_batch({1, 2, 3}, {21.0, 22.0, 23.0}));

  if (reader.read(array_stream)) {
    quill::error(logger, "reader should not read record batch");
    return 1;
  }

  auto writer2 = db.writer(1);
  writer2.write(create_batch({4, 5, 6}, {24.0, 25.0, 26.0}));

  if (!reader.read(array_stream)) {
    quill::error(logger, "reader should read record batch");
    return 1;
  }

  WriteStreamToFile("stream.arrow", *array_stream.get());
  return 0;
}
