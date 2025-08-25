#include "arrow_mmap/arrow_writer.hpp"

#include <libassert/assert.hpp>

namespace arrow_mmap {

ArrowWriter::ArrowWriter(const size_t id, const ArrowMeta meta, const IMmapWriter* data_writer,
                         const IMmapWriter* bitmap_writer)
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
          col_array_offsets.push_back(std::accumulate(col_sizes_.begin(), col_sizes_.begin() + i, 0) *
                                      meta_.array_length);
        }
        return col_array_offsets;
      }()),
      batch_size_(std::accumulate(col_sizes_.begin(), col_sizes_.end(), 0) * meta_.array_length) {}

bool ArrowWriter::write(const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto ret = write(batch, index_);
  if (ret) index_++;
  return ret;
}

bool ArrowWriter::write(const std::shared_ptr<arrow::RecordBatch>& batch, const size_t index) {
  ASSERT(index < meta_.capacity, "index out of range, index: {}, capacity: {}", index, meta_.capacity);
  ASSERT(batch->schema()->Equals(meta_.schema), "batch schema is not equal to meta schema");
  ASSERT(batch->num_rows() == write_rows, "batch num_rows: {} != write_rows: {}", batch->num_rows(), write_rows);

  auto target_batch_addr = data_writer_->mmap_addr() + index * batch_size_;
  for (size_t col_id = 0; col_id < col_sizes_.size(); col_id++) {
    auto col_array_size = col_array_sizes_[col_id];
    auto col_addr = target_batch_addr + col_array_offsets_[col_id];
    auto col_writer_addr = col_addr + id * col_array_size;
    auto col_array_data = batch->column(col_id)->data()->buffers[1];
    std::memcpy(col_writer_addr, reinterpret_cast<std::byte*>(col_array_data->address()), col_array_data->size());
  }

  // mark the index of current writer is written
  auto bitmap_addr = bitmap_writer_->mmap_addr();
  bitmap_addr[index * meta_.writer_count + id] = std::byte(0xff);
  return true;
}
}  // namespace arrow_mmap
