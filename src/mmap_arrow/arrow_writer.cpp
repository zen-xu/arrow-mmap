#include "mmap_arrow/arrow_writer.hpp"

#include <libassert/assert.hpp>

namespace mmap_arrow {

bool ArrowWriter::write(const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto ret = write(batch, index_);
  if (ret) index_++;
  return ret;
}

bool ArrowWriter::write(const std::shared_ptr<arrow::RecordBatch>& batch, size_t index) {
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
}  // namespace mmap_arrow
