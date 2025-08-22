#include <iostream>

#include "mmap_arrow/arrow_manager.hpp"
#include "mmap_arrow/arrow_meta.hpp"

int main() {
  auto manager = mmap_arrow::ArrowManager::create(
      "demo", 4, 100, 5, arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::float32())}));

  auto meta = manager.meta();
  std::cout << meta.to_string() << std::endl;
  return 0;
}
