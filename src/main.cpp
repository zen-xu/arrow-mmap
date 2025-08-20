#include <cstring>
#include <format>
#include <iostream>

#include "mmap_arrow/arrow_manager.hpp"

int main() {
  auto meta = mmap_arrow::ArrowMeta{
      .writer_count = 10,
      .array_length = 100,
      .capacity = 1000,
      .schema = arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::float32())}),
  };

  meta.serialize("meta.bin");
  auto meta2 = mmap_arrow::ArrowMeta::deserialize("meta.bin");
  std::cout << meta2.to_string() << std::endl;
  return 0;
}
