#include <mmap_arrow/core.hpp>

int test(mmap_arrow::MmapManager& manager) { return 0; }

int main() {
  mmap_arrow::MmapManager manager =
      mmap_arrow::MmapManager::create("tmp/test.mmap", 2048, {.fill_with = std::byte(0xff)});
  test(manager);
  return 0;
}
