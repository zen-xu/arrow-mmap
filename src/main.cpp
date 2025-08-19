#include <cstring>
#include <format>
#include <iostream>
#include <mmap_arrow/core.hpp>

int main() {
  mmap_arrow::MmapManager manager = mmap_arrow::MmapManager::create("tmp.mmap", 100);
  auto reader = manager.reader();
  auto writer = manager.writer();
  auto writer_addr = writer->mmap_addr();
  std::memcpy(writer_addr, "hello", 5);

  auto addr = reader->mmap_addr();
  char buf[10];
  std::memcpy(buf, addr, 5);
  std::print(std::cout, "buf: {}\n", buf);

  return 0;
}
