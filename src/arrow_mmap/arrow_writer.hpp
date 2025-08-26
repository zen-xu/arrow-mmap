#ifndef ARROW_MMAP_ARROW_WRITER_HPP
#define ARROW_MMAP_ARROW_WRITER_HPP
#pragma once

#include <arrow/api.h>

#include "arrow_mmap/arrow_meta.hpp"
#include "arrow_mmap/interface.hpp"

namespace arrow_mmap {

class ArrowWriter {
 public:
  ArrowWriter(const size_t id, const ArrowMeta meta, const IMmapWriter* data_writer, const IMmapWriter* bitflag_writer);

  bool write(const std::shared_ptr<arrow::RecordBatch>& batch);
  bool write(const std::shared_ptr<arrow::RecordBatch>& batch, const size_t index);

  const size_t current_index() const noexcept { return index_; }

  const size_t write_rows;
  const size_t id;

 private:
  size_t index_ = 0;
  const ArrowMeta meta_;
  const IMmapWriter* data_writer_;
  const IMmapWriter* bitflag_writer_;
  const std::vector<size_t> col_sizes_;
  const std::vector<size_t> col_array_sizes_;
  const std::vector<size_t> col_array_offsets_;
  const size_t batch_size_;
};

}  // namespace arrow_mmap
#endif  // ARROW_MMAP_ARROW_WRITER_HPP
