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
  ArrowWriter(MmapWriter data_writer, MmapWriter mask_writer, size_t writer_id, size_t capacity, size_t array_length,
              std::vector<size_t> rb_column_sizes, size_t writer_count, quill::Logger* logger)
      : data_writer_(data_writer),
        mask_writer_(mask_writer),
        writer_id_(writer_id),
        writer_count_(writer_count),
        capacity_(capacity),
        array_length_(array_length),
        rb_size_(std::accumulate(rb_column_sizes.begin(), rb_column_sizes.end(), 0) * array_length),
        rb_column_sizes_(rb_column_sizes),
        rb_column_chunk_sizes_([&]() {
          std::vector<size_t> sizes;
          for (size_t i = 0; i < rb_column_sizes.size(); i++) {
            sizes.push_back(rb_column_sizes[i] * array_length / writer_count);
          }
          return sizes;
        }()),
        rb_column_offsets_([&]() {
          std::vector<size_t> offsets;
          for (size_t i = 0; i < rb_column_sizes.size(); i++) {
            offsets.push_back(std::accumulate(rb_column_sizes.begin(), rb_column_sizes.begin() + i, 0) * array_length);
          }
          return offsets;
        }()),
        logger_(logger) {}

  const std::byte* data_addr() const { return data_writer_.mmap_addr(); }
  const MmapWriter& data_writer() const { return data_writer_; }

  bool write(const std::shared_ptr<::arrow::RecordBatch>& record_batch_chunk) {
    auto ret = write(record_batch_chunk, index_);
    if (ret) {
      index_++;
    }
    return ret;
  }

  bool write(const std::shared_ptr<::arrow::RecordBatch>& record_batch_chunk, size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to write: index {} >= capacity {}", index, capacity_);
      return false;
    }

    // the address of the record batch
    auto rb_addr = data_writer_.mmap_addr() + index * rb_size_;

    for (size_t col_id = 0; col_id < record_batch_chunk->num_columns(); col_id++) {
      auto chunk_col_size = rb_column_chunk_sizes_[col_id];
      auto rb_col_addr = rb_addr + rb_column_offsets_[col_id];
      auto rb_col_writer_addr = rb_col_addr + writer_id_ * chunk_col_size;
      auto col_array = record_batch_chunk->column(col_id)->data();
      auto col_array_data = col_array->buffers[1];
      std::memcpy(rb_col_writer_addr, reinterpret_cast<uint8_t*>(col_array_data->address()), col_array_data->size());
    }

    // mark current writer row is written
    auto mask_addr = mask_writer_.mmap_addr();
    mask_addr[index * writer_count_ + writer_id_] = std::byte(0xFF);
    return true;
  }

 private:
  const MmapWriter data_writer_;
  const MmapWriter mask_writer_;
  const size_t writer_id_;
  const size_t writer_count_;
  const size_t capacity_;
  const size_t array_length_;
  const size_t rb_size_;
  const std::vector<size_t> rb_column_sizes_;
  const std::vector<size_t> rb_column_chunk_sizes_;
  const std::vector<size_t> rb_column_offsets_;
  quill::Logger* logger_;
  size_t index_ = 0;
};

class ArrowReader {
 public:
  ArrowReader(MmapReader data_reader, MmapReader mask_reader, size_t writer_count, size_t capacity, size_t array_length,
              std::shared_ptr<::arrow::Schema> schema, quill::Logger* logger)
      : data_reader_(data_reader),
        mask_reader_(mask_reader),
        writer_count_(writer_count),
        capacity_(capacity),
        array_length_(array_length),
        rb_size_(std::accumulate(schema->fields().begin(), schema->fields().end(), 0,
                                 [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); })),
        schema_(schema),
        logger_(logger) {}

  std::shared_ptr<::arrow::RecordBatch> read() { return read(index_); }
  std::shared_ptr<::arrow::RecordBatch> read(size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to read: index {} >= capacity {}", index, capacity_);
      return nullptr;
    }
    auto mask_size = writer_count_;

    auto mask_addr = mask_reader_.read(mask_size, mask_size * index);
    if (mask_addr == nullptr) {
      return nullptr;
    }
    if (!std::all_of(mask_addr, mask_addr + mask_size, [](std::byte b) { return b == std::byte(0xFF); })) {
      return nullptr;
    }

    // all writer have written this record batch
    auto arrays = std::vector<std::shared_ptr<::arrow::Array>>();
    auto rb_addr = data_reader_.mmap_addr() + index * rb_size_;
    for (int i = 0; i < schema_->num_fields(); i++) {
      auto field = schema_->field(i);
      auto filed_array_size = field->type()->byte_width() * array_length_;
      auto array_data = ::arrow::ArrayData::Make(
          field->type(), array_length_,
          {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(rb_addr), filed_array_size)});
      arrays.push_back(::arrow::MakeArray(array_data));
      rb_addr += filed_array_size;
    }
    return ::arrow::RecordBatch::Make(schema_, 1, arrays);
  }

 private:
  const MmapReader data_reader_;
  const MmapReader mask_reader_;
  const size_t writer_count_;
  const size_t capacity_;
  const size_t array_length_;
  const size_t rb_size_;
  const std::shared_ptr<::arrow::Schema> schema_;
  size_t index_ = 0;
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
      schema_ifs.read(reinterpret_cast<char*>(&capacity_), sizeof(capacity_));
      schema_ifs.read(reinterpret_cast<char*>(&array_length_), sizeof(array_length_));
      std::vector<char> schema_data_vec(std::istreambuf_iterator<char>(schema_ifs), {});
      schema_ifs.close();
      auto schema_buffer = ::arrow::Buffer::FromString(std::string(schema_data_vec.begin(), schema_data_vec.end()));
      auto reader = ::arrow::io::BufferReader(schema_buffer);
      schema_ = ::arrow::ipc::ReadSchema(&reader, nullptr).ValueOrDie();
    }
  }

  size_t writer_count() const { return writer_count_; }
  size_t capacity() const { return capacity_; }
  std::shared_ptr<::arrow::Schema> schema() const { return schema_; }

  // schema is the element type of array
  bool create(size_t writer_count, size_t capacity, size_t array_length, std::shared_ptr<::arrow::Schema> schema) {
    if (writer_count == 0) {
      quill::error(logger_, "fail to create: writer_count can't be 0");
      return false;
    }
    if (capacity == 0) {
      quill::error(logger_, "fail to create: capacity can't be 0");
      return false;
    }
    if (array_length % writer_count != 0) {
      quill::error(logger_, "fail to create: array_length must be divisible by writer_count");
      return false;
    }
    if (array_length == 0) {
      quill::error(logger_, "fail to create: array_length can't be 0");
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
      schema_ofs.write(reinterpret_cast<const char*>(&capacity), sizeof(size_t));
      schema_ofs.write(reinterpret_cast<const char*>(&array_length), sizeof(size_t));
      schema_ofs.write(reinterpret_cast<const char*>(schema_buffer->data()), schema_buffer->size());
      schema_ofs.close();
    }

    // create data and mask file
    auto create_file = [&]() -> bool {
      auto data_capacity =
          capacity * array_length *
          std::accumulate(schema->fields().begin(), schema->fields().end(), 0,
                          [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); });
      if (!::mmap_db::truncate(data_path_, data_capacity, true)) {
        quill::error(logger_, "fail to truncate data: {}", data_path_);
        return false;
      }

      // make sure creating mask file is atomic operation
      auto mask_tmp_path = mask_path_ + ".tmp";
      auto mask_capacity = capacity * writer_count;
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
    capacity_ = capacity;
    array_length_ = array_length;
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
    return ArrowWriter(data_manager_.writer(), mask_manager_.writer(), writer_id, capacity_, array_length_,
                       column_bit_widths, writer_count_, logger_);
  }

  ArrowReader reader() {
    return ArrowReader(data_manager_.reader(), mask_manager_.reader(), writer_count_, capacity_, array_length_, schema_,
                       logger_);
  }

 private:
  std::string data_path_;
  std::string mask_path_;
  std::string schema_path_;
  size_t writer_count_ = 0;
  size_t capacity_ = 0;
  size_t array_length_ = 0;
  MmapManager data_manager_;
  MmapManager mask_manager_;
  std::shared_ptr<::arrow::Schema> schema_ = nullptr;
  quill::Logger* logger_;
};
}  // namespace mmap_db::arrow

#endif  // MMAP_DB_ARROW_DB_HPP
