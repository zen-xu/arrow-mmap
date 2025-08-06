#include <arrow/api.h>
#include <arrow/io/api.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/dyn_partition_db.hpp"

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));
  auto db = mmap_db::DynPartitionDB<mmap_db::DynPartitionOrder::F>("arrow_db");
  constexpr int64_t num_rows = 3;
  constexpr int64_t fixed_size = 8;

  auto schema =
      arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::fixed_size_binary(fixed_size))});
  db.create(num_rows, [schema, num_rows]() {
    std::vector<size_t> f_byte_width;
    for (auto field : schema->fields()) {
      f_byte_width.push_back(field->type()->byte_width());
    }
    return f_byte_width;
  }());
  db.truncate(num_rows, true);

  auto id_writer = db.writer(0);
  int32_t id1 = 1;
  int32_t id2 = 2;
  int32_t id3 = 3;
  id_writer.write(&id1);
  id_writer.write(&id2);
  id_writer.write(&id3);
  auto id_writer_addr = id_writer.addr(0);
  auto id_array_data = arrow::ArrayData::Make(arrow::int32(), num_rows);
  id_array_data->buffers = {
      nullptr, std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(id_writer_addr), num_rows * sizeof(int32_t))};
  id_array_data->length = num_rows;
  auto id_array = arrow::MakeArray(id_array_data);

  auto name_writer = db.writer(1);
  std::string name1 = "Alice";
  std::string name2 = "Bob";
  std::string name3 = "Charlie";
  name_writer.write(name1.c_str());
  name_writer.write(name2.c_str());
  name_writer.write(name3.c_str());
  auto name_writer_addr = name_writer.addr(0);
  auto name_array_data = arrow::ArrayData::Make(arrow::fixed_size_binary(fixed_size), num_rows);
  name_array_data->buffers = {
      nullptr, std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(name_writer_addr), num_rows * fixed_size)};
  name_array_data->length = num_rows;
  auto name_array = arrow::MakeArray(name_array_data);

  auto batch = arrow::RecordBatch::Make(schema, num_rows, {id_array, name_array});
  for (int i = 0; i < batch->num_rows(); ++i) {
    auto id_scalar = batch->column(0)->GetScalar(i).ValueOrDie();
    auto name_scalar = batch->column(1)->GetScalar(i).ValueOrDie();
    quill::info(logger, "id: {}, name: {}", id_scalar->ToString(), name_scalar->ToString());
  }
}
