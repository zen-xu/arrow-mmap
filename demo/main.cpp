#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

struct Row {
  int32_t id;
  char c;
} __attribute__((packed));

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::fixed_size_binary(1))});
  auto rows = 4;
  auto db = mmap_db::arrow::ArrowDB("arrow_db");
  db.create(2, rows, schema);

  Row row1{1, 'a'};
  Row row2{2, 'b'};

  auto writer0 = db.writer(0);
  auto writer1 = db.writer(1);
  auto reader = db.reader();

  writer0.write(&row1);
  auto batch = reader.read();
  if (batch == nullptr) {
    quill::info(logger, "batch is nullptr");
  } else {
    quill::error(logger, "batch is not nullptr");
  }
  writer1.write(&row2);

  batch = reader.read();
  if (batch == nullptr) {
    quill::error(logger, "batch is nullptr");
  } else {
    for (int i = 0; i < batch->num_rows(); i++) {
      auto id_scalar = batch->column(0)->GetScalar(i).ValueOrDie();
      auto name_scalar = batch->column(1)->GetScalar(i).ValueOrDie();
      quill::info(logger, "batch id: {}, name: {}", id_scalar->ToString(), name_scalar->ToString());
    }
  }

  return 0;
}
