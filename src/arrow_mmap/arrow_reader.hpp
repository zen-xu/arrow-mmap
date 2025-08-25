#ifndef ARROW_MMAP_ARROW_READER_HPP
#define ARROW_MMAP_ARROW_READER_HPP
#pragma once

#include <nanoarrow/nanoarrow.hpp>

#include "arrow_meta.hpp"
#include "interface.hpp"

namespace arrow_mmap {
class ArrowReader {
 public:
  ArrowReader(const ArrowMeta meta, const IMmapReader* data_reader, const IMmapReader* bitmap_reader);

  bool read(nanoarrow::UniqueArrayStream& stream);
  bool read(nanoarrow::UniqueArrayStream& stream, const size_t index);

  const size_t current_index() const noexcept { return index_; }

 private:
  const ArrowMeta meta_;
  const IMmapReader* data_reader_;
  const IMmapReader* bitmap_reader_;
  const size_t batch_size_;
  const std::vector<size_t> col_sizes_;
  const std::vector<ArrowType> col_types_;

  size_t index_ = 0;
  nanoarrow::UniqueSchema schema_;
  nanoarrow::UniqueArray struct_array_;
};
}  // namespace arrow_mmap
#endif  // ARROW_MMAP_ARROW_READER_HPP
