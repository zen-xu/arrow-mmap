#include <arrow/api.h>
#include <arrow/io/api.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

struct Row {
  int32_t id;
  char c;
} __attribute__((packed));

std::shared_ptr<arrow::RecordBatch> make_record_batch(mmap_db::arrow::ArrowWriter& writer,
                                                      std::shared_ptr<arrow::Schema> schema, int32_t rows) {
  auto arrays = std::vector<std::shared_ptr<arrow::Array>>();
  for (int i = 0; i < schema->num_fields(); i++) {
    auto field = schema->field(i);
    auto array_data = arrow::ArrayData::Make(field->type(), rows);
    array_data->buffers = {nullptr, std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(writer.col_addr(i)),
                                                                    rows * field->type()->byte_width())};
    array_data->length = rows;
    arrays.push_back(arrow::MakeArray(array_data));
  }
  return arrow::RecordBatch::Make(schema, rows, arrays);
}

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  auto rows = 4;
  auto db = mmap_db::arrow::ArrowDB("arrow_db");
  auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::fixed_size_binary(1))});
  db.create(2, rows, schema);

  auto writer0 = db.writer(0);
  Row row1{1, 'a'};
  Row row2{2, 'b'};
  Row row3{3, 'c'};
  Row row4{4, 'd'};
  writer0.write(&row1);
  writer0.write(&row2);
  writer0.write(&row3);
  writer0.write(&row4);

  auto writer1 = db.writer(1);
  Row row5{5, 'e'};
  Row row6{6, 'f'};
  Row row7{7, 'g'};
  Row row8{8, 'h'};
  writer1.write(&row5);
  writer1.write(&row6);
  writer1.write(&row7);
  writer1.write(&row8);

  auto batch0 = make_record_batch(writer0, schema, rows);
  auto batch1 = make_record_batch(writer1, schema, rows);

  for (int i = 0; i < batch0->num_rows(); i++) {
    auto id_scalar = batch0->column(0)->GetScalar(i).ValueOrDie();
    auto name_scalar = batch0->column(1)->GetScalar(i).ValueOrDie();
    quill::info(logger, "batch0 id: {}, name: {}", id_scalar->ToString(), name_scalar->ToString());
  }
  for (int i = 0; i < batch1->num_rows(); i++) {
    auto id_scalar = batch1->column(0)->GetScalar(i).ValueOrDie();
    auto name_scalar = batch1->column(1)->GetScalar(i).ValueOrDie();
    quill::info(logger, "batch1 id: {}, name: {}", id_scalar->ToString(), name_scalar->ToString());
  }
  return 0;
}
