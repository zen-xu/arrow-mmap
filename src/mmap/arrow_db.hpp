#ifndef MMAP_DB_ARROW_DB_HPP
#define MMAP_DB_ARROW_DB_HPP

#include <filesystem>
#include <fstream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "manager.hpp"

namespace mmap_db::arrow {
struct ArrowWriterConfig {
  int writer_flags = 0;
  int reader_flags = 0;
};

class ArrowWriter {
 public:
  ArrowWriter(MmapWriter data_writer, MmapWriter mask_writer, size_t writer_id, size_t batch_count,
              std::vector<size_t> column_bit_widths, size_t writer_count, quill::Logger* logger)
      : data_writer_(data_writer),
        mask_writer_(mask_writer),
        writer_id_(writer_id),
        writer_count_(writer_count),
        batch_count_(batch_count),
        batch_chunk_size_(std::accumulate(column_bit_widths.begin(), column_bit_widths.end(), 0) * writer_count),
        column_bit_widths_(column_bit_widths),
        logger_(logger) {
    for (size_t i = 0; i < column_bit_widths.size(); i++) {
      column_bit_offsets_.push_back(std::accumulate(column_bit_widths.begin(), column_bit_widths.begin() + i, 0) *
                                    writer_count);
    }
  }

  template <typename T>
  bool write(const T& batch_data) {
    return write(std::move(batch_data));
  }

  template <typename T>
  bool write(const T&& batch_data) {
    auto ret = write(std::move(batch_data), current_batch_);
    if (ret) {
      current_batch_++;
    }
    return ret;
  }

  template <typename T>
  bool write(const T& batch_data, size_t batch) {
    return write(std::move(batch_data), batch);
  }

  template <typename T>
  bool write(const T&& batch_data, size_t batch) {
    if (batch >= batch_count_) {
      quill::error(logger_, "failed to write: batch {} >= batch count {}", batch, batch_count_);
      return false;
    }
    auto batch_data_addr = data_writer_.mmap_addr() + batch * sizeof(T) * writer_count_;
    auto batch_data_ptr = reinterpret_cast<const std::byte*>(&batch_data);
    for (size_t col_id = 0; col_id < column_bit_widths_.size(); col_id++) {
      auto col_bit_width = column_bit_widths_[col_id];
      auto batch_addr = batch_data_addr + column_bit_offsets_[col_id] * sizeof(std::byte);
      auto batch_worker_addr = batch_addr + writer_id_ * col_bit_width * sizeof(std::byte);
      memcpy(batch_worker_addr, batch_data_ptr, col_bit_width * sizeof(std::byte));
      batch_data_ptr = batch_data_ptr + col_bit_width * sizeof(std::byte);
    }

    // mark current writer row is written
    auto mask_addr = mask_writer_.mmap_addr();
    mask_addr[batch * writer_count_ + writer_id_] = std::byte(0xFF);
    return true;
  }

 private:
  MmapWriter data_writer_;
  MmapWriter mask_writer_;
  size_t writer_id_;
  size_t writer_count_;
  size_t current_batch_ = 0;
  size_t batch_count_;
  size_t batch_chunk_size_;
  std::vector<size_t> column_bit_widths_ = {};
  std::vector<size_t> column_bit_offsets_ = {};
  quill::Logger* logger_;
};

class ArrowReader {
 public:
  ArrowReader(MmapReader data_reader, MmapReader mask_reader, size_t writer_count, size_t batch_count,
              std::shared_ptr<::arrow::Schema> schema, quill::Logger* logger)
      : data_reader_(data_reader),
        mask_reader_(mask_reader),
        writer_count_(writer_count),
        batch_count_(batch_count),
        batch_chunk_size_(
            std::accumulate(schema->fields().begin(), schema->fields().end(), 0,
                            [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); })),
        schema_(schema),
        logger_(logger) {}

  std::shared_ptr<::arrow::RecordBatch> read() { return read(current_batch_); }
  std::shared_ptr<::arrow::RecordBatch> read(size_t batch) {
    if (batch >= batch_count_) {
      quill::error(logger_, "failed to read: batch {} >= batch count {}", batch, batch_count_);
      return nullptr;
    }
    auto mask_size = sizeof(std::byte) * writer_count_;
    auto mask_addr = mask_reader_.read(mask_size, mask_size * batch);
    if (mask_addr == nullptr) {
      return nullptr;
    }
    if (!std::all_of(mask_addr, mask_addr + mask_size, [](std::byte b) { return b == std::byte(0xFF); })) {
      return nullptr;
    }

    // all writer has written this row
    auto arrays = std::vector<std::shared_ptr<::arrow::Array>>();
    auto batch_addr = data_reader_.mmap_addr() + batch * batch_chunk_size_ * sizeof(std::byte);
    for (int i = 0; i < schema_->num_fields(); i++) {
      auto field = schema_->field(i);
      auto array_data = ::arrow::ArrayData::Make(field->type(), writer_count_);
      array_data->buffers = {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<uint8_t*>(batch_addr),
                                                                        field->type()->byte_width() * writer_count_)};
      array_data->length = writer_count_;
      arrays.push_back(::arrow::MakeArray(array_data));
      batch_addr = batch_addr + writer_count_ * field->type()->byte_width() * sizeof(std::byte);
    }
    return ::arrow::RecordBatch::Make(schema_, writer_count_, arrays);
  }

 private:
  MmapReader data_reader_;
  MmapReader mask_reader_;
  size_t writer_count_;
  size_t batch_count_;
  size_t batch_chunk_size_;
  size_t current_batch_ = 0;
  std::shared_ptr<::arrow::Schema> schema_;
  quill::Logger* logger_;
};

class ArrowDB {
 public:
  ArrowDB(const std::string& path, ArrowWriterConfig config = ArrowWriterConfig())
      : data_path_(std::filesystem::path(path) / "arrow_data.mmap"),
        mask_path_(std::filesystem::path(path) / "arrow_mask.mmap"),
        schema_path_(std::filesystem::path(path) / "arrow_schema.bin"),
        data_manager_(data_path_, config.reader_flags, config.writer_flags),
        mask_manager_(mask_path_, config.reader_flags, config.writer_flags),
        logger_(quill::Frontend::create_or_get_logger("default")) {
    if (std::filesystem::exists(schema_path_)) {
      std::ifstream schema_ifs(schema_path_, std::ios::binary);
      schema_ifs.read(reinterpret_cast<char*>(&writer_count_), sizeof(writer_count_));
      schema_ifs.read(reinterpret_cast<char*>(&batch_count_), sizeof(batch_count_));
      std::vector<char> schema_data_vec(std::istreambuf_iterator<char>(schema_ifs), {});
      schema_ifs.close();
      auto schema_buffer = ::arrow::Buffer::FromString(std::string(schema_data_vec.begin(), schema_data_vec.end()));
      auto reader = ::arrow::io::BufferReader(schema_buffer);
      schema_ = ::arrow::ipc::ReadSchema(&reader, nullptr).ValueOrDie();
    }
  }

  size_t writer_count() const { return writer_count_; }
  size_t rows() const { return batch_count_; }
  std::shared_ptr<::arrow::Schema> schema() const { return schema_; }

  bool create(size_t writer_count, size_t batch_count, std::shared_ptr<::arrow::Schema> schema) {
    if (writer_count == 0) {
      quill::error(logger_, "fail to create: writer_count can't be 0");
      return false;
    }
    if (batch_count == 0) {
      quill::error(logger_, "fail to create: batch_count can't be 0");
      return false;
    }
    if (schema == nullptr) {
      quill::error(logger_, "fail to create: schema can't be nullptr");
      return false;
    }

    std::filesystem::remove(schema_path_);
    std::filesystem::remove(data_path_);
    std::filesystem::remove(mask_path_);

    auto path_dir = std::filesystem::path(schema_path_).parent_path();
    if (!std::filesystem::exists(path_dir) && !std::filesystem::create_directories(path_dir)) {
      quill::error(logger_, "failed to create directory: {}", path_dir.c_str());
      return false;
    }

    // dump schema
    std::ofstream schema_ofs(schema_path_, std::ios::binary);
    if (!schema_ofs) {
      quill::error(logger_, "failed to open schema file for writing: {}", schema_path_);
      return false;
    } else {
      auto schema_buffer = ::arrow::ipc::SerializeSchema(*schema).ValueOrDie();
      schema_ofs.write(reinterpret_cast<const char*>(&writer_count), sizeof(size_t));
      schema_ofs.write(reinterpret_cast<const char*>(&batch_count), sizeof(size_t));
      schema_ofs.write(reinterpret_cast<const char*>(schema_buffer->data()), schema_buffer->size());
      schema_ofs.close();
    }

    // create data and mask file
    auto create_file = [&]() -> bool {
      auto data_capacity =
          sizeof(std::byte) * batch_count * writer_count *
          std::accumulate(schema->fields().begin(), schema->fields().end(), 0,
                          [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); });
      if (!::mmap_db::truncate(data_path_, data_capacity, true)) {
        quill::error(logger_, "fail to truncate data: {}", data_path_);
        return false;
      }

      // make sure creating mask file is atomic operation
      auto mask_tmp_path = mask_path_ + ".tmp";
      auto mask_capacity = batch_count * writer_count * sizeof(std::byte);
      if (!::mmap_db::truncate(mask_tmp_path, mask_capacity, true)) {
        quill::error(logger_, "fail to truncate mask: {}", mask_path_);
        std::filesystem::remove(mask_tmp_path);
        return false;
      }
      std::filesystem::rename(mask_tmp_path, mask_path_);
      return true;
    };
    if (!create_file()) {
      // remove data_path, mask_path and schema_path
      std::filesystem::remove(data_path_);
      std::filesystem::remove(mask_path_);
      std::filesystem::remove(schema_path_);
      return false;
    }
    writer_count_ = writer_count;
    batch_count_ = batch_count;
    schema_ = schema;
    return true;
  }

  ArrowWriter writer(size_t writer_id) {
    if (writer_id >= writer_count_) {
      throw std::runtime_error(std::format("writer_id out of range: {} >= {}", writer_id, writer_count_));
    }
    std::vector<size_t> column_bit_widths;
    for (auto field : schema_->fields()) {
      column_bit_widths.push_back(field->type()->byte_width());
    }
    return ArrowWriter(data_manager_.writer(), mask_manager_.writer(), writer_id, batch_count_, column_bit_widths,
                       writer_count_, logger_);
  }

  ArrowReader reader() {
    return ArrowReader(data_manager_.reader(), mask_manager_.reader(), writer_count_, batch_count_, schema_, logger_);
  }

 private:
  std::string data_path_;
  std::string mask_path_;
  std::string schema_path_;
  size_t writer_count_ = 0;
  size_t batch_count_ = 0;
  MmapManager data_manager_;
  MmapManager mask_manager_;
  std::shared_ptr<::arrow::Schema> schema_ = nullptr;
  quill::Logger* logger_;
};
}  // namespace mmap_db::arrow

#endif  // MMAP_DB_ARROW_DB_HPP
