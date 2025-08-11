#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

int main() {
  auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("age", arrow::int32())});

  // 创建第一个 batch
  std::shared_ptr<arrow::Array> id_array1;
  std::shared_ptr<arrow::Array> age_array1;
  arrow::Int32Builder id_builder1, age_builder1;
  id_builder1.AppendValues({1, 2, 3, 4, 5});
  age_builder1.AppendValues({21, 22, 23, 24, 25});
  id_builder1.Finish(&id_array1);
  age_builder1.Finish(&age_array1);
  auto batch1 = arrow::RecordBatch::Make(schema, 5, {id_array1, age_array1});

  // 创建第二个 batch
  std::shared_ptr<arrow::Array> id_array2;
  std::shared_ptr<arrow::Array> age_array2;
  arrow::Int32Builder id_builder2, age_builder2;
  id_builder2.AppendValues({6, 7, 8, 9, 10});
  age_builder2.AppendValues({26, 27, 28, 29, 30});
  id_builder2.Finish(&id_array2);
  age_builder2.Finish(&age_array2);
  auto batch2 = arrow::RecordBatch::Make(schema, 5, {id_array2, age_array2});

  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  auto db = mmap_db::arrow::ArrowDB("arrow_db");
  db.create(2, 1, 10, schema);
  auto writer1 = db.writer(0);
  auto writer2 = db.writer(1);
  auto reader = db.reader();
  std::shared_ptr<arrow::RecordBatch> result;
  writer1.write(batch1);
  result = reader.read();
  if (nullptr != result) {
    quill::error(logger, "result is not null");
    return 1;
  }
  writer2.write(batch2);
  result = reader.read();
  if (nullptr == result) {
    quill::error(logger, "result is null");
    return 1;
  }
  quill::info(logger, "result:\n{}", result->ToString());

  return 0;
}
