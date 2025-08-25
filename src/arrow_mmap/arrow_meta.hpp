#ifndef MMAP_ARROW_ARROW_META_HPP
#define MMAP_ARROW_ARROW_META_HPP
#pragma once

#include <arrow/api.h>

namespace mmap_arrow {

struct ArrowMeta {
  size_t writer_count;
  size_t array_length;
  size_t capacity;
  std::shared_ptr<arrow::Schema> schema;

  std::string to_string() const;

  void serialize(std::ofstream& ofs) const;
  void serialize(const std::string& output_file) const;
  static ArrowMeta deserialize(std::ifstream& ifs);
  static ArrowMeta deserialize(const std::string& input_file);
};

}  // namespace mmap_arrow
#endif
