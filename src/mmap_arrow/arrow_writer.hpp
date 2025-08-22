#ifndef MMAP_ARROW_ARROW_WRITER_HPP
#define MMAP_ARROW_ARROW_WRITER_HPP
#pragma once

#include <arrow/api.h>

#include "mmap_arrow/arrow_meta.hpp"
#include "mmap_arrow/interface.hpp"

namespace mmap_arrow {

class ArrowWriter {
 public:
  ArrowWriter(const size_t id, const ArrowMeta meta, const IMmapWriter* data_writer, const IMmapWriter* bitmap_writer)
      : id(id),
        meta_(meta),
        data_writer_(data_writer),
        bitmap_writer_(bitmap_writer),
        write_rows([id, meta]() {
          if (id < meta.writer_count - 1) {
            return meta.array_length / meta.writer_count;
          } else {
            return meta.array_length - meta.array_length / meta.writer_count * (meta.writer_count - 1);
          }
        }()),
        col_sizes_([&]() {
          std::vector<size_t> col_sizes;
          for (const auto& field : meta.schema->fields()) {
            col_sizes.push_back(field->type()->byte_width());
          }
          return col_sizes;
        }()),
        col_array_sizes_([&]() {
          std::vector<size_t> col_array_sizes;
          // Because except for the last writer, the num_rows of other writers is the same
          auto num_rows = meta_.array_length / meta_.writer_count;
          for (const auto& size : col_sizes_) {
            col_array_sizes.push_back(size * num_rows);
          }
          return col_array_sizes;
        }()),
        col_array_offsets_([&]() {
          std::vector<size_t> col_array_offsets;
          for (size_t i = 0; i < col_sizes_.size(); i++) {
            col_array_offsets.push_back(std::accumulate(col_sizes_.begin(), col_sizes_.begin() + i, 0) * write_rows);
          }
          return col_array_offsets;
        }()),
        batch_size_(std::accumulate(col_sizes_.begin(), col_sizes_.end(), 0) * meta_.array_length) {}

  bool write(const std::shared_ptr<arrow::RecordBatch>& batch);
  bool write(const std::shared_ptr<arrow::RecordBatch>& batch, size_t index);

  size_t current_index() const noexcept { return index_; }

  const size_t write_rows;
  const size_t id;

 private:
  size_t index_ = 0;
  const ArrowMeta meta_;
  const IMmapWriter* data_writer_;
  const IMmapWriter* bitmap_writer_;
  const std::vector<size_t> col_sizes_;
  const std::vector<size_t> col_array_sizes_;
  const std::vector<size_t> col_array_offsets_;
  const size_t batch_size_;
};

}  // namespace mmap_arrow
#endif
