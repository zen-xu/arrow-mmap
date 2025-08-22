#include <iostream>
#include <libassert/assert.hpp>

#include "mmap_arrow/arrow_manager.hpp"
#include "mmap_arrow/arrow_meta.hpp"
#include "mmap_arrow/arrow_reader.hpp"

const auto SCHEMA = arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::float32())});

const std::shared_ptr<arrow::RecordBatch> create_batch(std::vector<int32_t> a, std::vector<float> b) {
  std::shared_ptr<arrow::Array> a_array;
  std::shared_ptr<arrow::Array> b_array;
  arrow::Int32Builder a_builder;
  arrow::FloatBuilder b_builder;
  a_builder.AppendValues(a).ok();
  b_builder.AppendValues(b).ok();
  a_builder.Finish(&a_array).ok();
  b_builder.Finish(&b_array).ok();
  return arrow::RecordBatch::Make(SCHEMA, a.size(), {a_array, b_array});
}

int main() {
  auto manager = mmap_arrow::ArrowManager::create("db", 2, 11, 5, SCHEMA);
  auto meta = manager.meta();
  std::cout << meta.to_string() << std::endl;

  auto writer0 = manager.writer(0);
  auto batch0 = create_batch({1, 2, 3, 4, 5}, {1.1, 1.2, 1.3, 1.4, 1.5});
  std::cout << "write batch0" << std::endl;
  writer0->write(batch0);
  std::cout << "write batch0 done" << std::endl;

  auto writer1 = manager.writer(1);
  auto batch1 = create_batch({6, 7, 8, 9, 10, 11}, {2.1, 2.2, 2.3, 2.4, 2.5, 2.6});
  std::cout << "write batch1" << std::endl;
  writer1->write(batch1);
  std::cout << "write batch1 done" << std::endl;

  return 0;
}
