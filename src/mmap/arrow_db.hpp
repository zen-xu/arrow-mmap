#ifndef MMAP_DB_ARROW_DB_HPP
#define MMAP_DB_ARROW_DB_HPP

#include <filesystem>

#include <arrow/api.h>

#include "manager.hpp"

namespace mmap_db::arrow {
struct ArrowWriterConfig {
  int writer_flags = 0;
  int reader_flags = 0;
};

// 只允许 Order 为 "C" 或 "F" 的辅助类型
enum class Order { C, F };

class ArrowWriter {
 public:
  ArrowWriter(MmapWriter data_writer, MmapWriter mask_writer, size_t writer_id, size_t rows,
              std::vector<size_t> column_bit_widths, size_t writer_count, quill::Logger* logger)
      : data_writer_(data_writer),
        mask_writer_(mask_writer),
        row_data_size_(std::accumulate(column_bit_widths.begin(), column_bit_widths.end(), 0)),
        writer_id_(writer_id),
        writer_count_(writer_count),
        writer_data_addr_(data_writer_.mmap_addr() + writer_id * row_data_size_ * rows * sizeof(std::byte)),
        rows_(rows),
        column_bit_widths_(column_bit_widths),
        column_bit_offsets_([&]() {
          std::vector<size_t> column_bit_offsets;
          for (size_t i = 0; i < column_bit_widths.size(); i++) {
            column_bit_offsets.push_back(std::accumulate(column_bit_widths.begin(), column_bit_widths.begin() + i, 0) *
                                         rows);
          }
          return column_bit_offsets;
        }()),
        logger_(logger) {}

  bool write(const void* row_data) {
    auto ret = write(row_data, row_id_);
    if (ret) {
      row_id_++;
    }
    return ret;
  }

  bool write(const void* row_data, size_t row_id) {
    if (row_id >= rows_) {
      quill::error(logger_, "failed to write: row_id {} >= total rows {}", row_id, rows_);
      return false;
    }

    auto row_data_ptr = reinterpret_cast<const std::byte*>(row_data);
    for (size_t col_id = 0; col_id < column_bit_widths_.size(); col_id++) {
      auto col_bit_width = column_bit_widths_[col_id];
      memcpy(element_addr(row_id, col_id), row_data_ptr, col_bit_width * sizeof(std::byte));
      row_data_ptr = row_data_ptr + col_bit_width * sizeof(std::byte);
    }

    // mark current writer row is written
    auto mask_addr = mask_writer_.mmap_addr();
    mask_addr[row_id * writer_count_ + writer_id_] = std::byte(0xFF);
    return true;
  }

  std::byte* col_addr(size_t col_id) { return element_addr(0, col_id); }

  std::byte* element_addr(size_t row_id, size_t col_id) {
    return writer_data_addr_ + column_bit_offsets_[col_id] * sizeof(std::byte) + row_id * column_bit_widths_[col_id];
  }

 private:
  MmapWriter data_writer_;
  MmapWriter mask_writer_;
  std::size_t row_data_size_;
  std::byte* writer_data_addr_;
  size_t writer_id_;
  size_t writer_count_;
  size_t row_id_ = 0;
  size_t rows_;
  std::vector<size_t> column_bit_widths_;
  std::vector<size_t> column_bit_offsets_;
  quill::Logger* logger_;
};

class ArrowDB {
 public:
  ArrowDB(const std::string& path, ArrowWriterConfig config = ArrowWriterConfig())
      : data_path_(std::filesystem::path(path) / "arrow_data.mmap"),
        mask_path_(std::filesystem::path(path) / "arrow_mask.mmap"),
        schema_path_(std::filesystem::path(path) / "arrow_schema.mmap"),
        data_manager_(data_path_, config.reader_flags, config.writer_flags),
        mask_manager_(mask_path_, config.reader_flags, config.writer_flags),
        logger_(quill::Frontend::create_or_get_logger("default")) {
    if (std::filesystem::exists(schema_path_)) {
      auto schema_manager = MmapManager(schema_path_);
      auto schema_reader = schema_manager.reader();

      auto buf = reinterpret_cast<std::size_t*>(schema_reader.read(schema_reader.length(), 0));
      if (nullptr != buf) {
        for (size_t i = 0; i < schema_reader.length() / sizeof(std::size_t); i++) {
          if (i == 0) {
            writer_count_ = buf[i];
          } else if (i == 1) {
            rows_ = buf[i];
          } else {
            column_bit_widths_.push_back(buf[i]);
          }
        }
      } else {
        quill::error(logger_, "failed to read schema");
      }
    }
  }

  bool create(size_t writer_count, size_t rows, std::shared_ptr<::arrow::Schema> schema) {
    std::vector<size_t> column_bit_widths;
    for (auto field : schema->fields()) {
      column_bit_widths.push_back(field->type()->byte_width());
    }
    return create(writer_count, rows, column_bit_widths);
  }

  bool create(size_t writer_count, size_t rows, std::vector<size_t> column_bit_widths) {
    if (writer_count == 0) {
      quill::error(logger_, "fail to create: writer_count can't be 0");
      return false;
    }
    if (rows == 0) {
      quill::error(logger_, "fail to create: rows can't be 0");
      return false;
    }
    if (column_bit_widths.empty()) {
      quill::error(logger_, "fail to create: column_bit_widths can't be empty");
      return false;
    }

    std::filesystem::remove(schema_path_);
    std::filesystem::remove(data_path_);
    std::filesystem::remove(mask_path_);

    auto schema_manager = MmapManager(schema_path_);
    // record schema
    if (!::mmap_db::truncate(schema_path_,
                             // init 2 is for writer_count and rows
                             (column_bit_widths.size() + 2) * sizeof(size_t))) {
      quill::error(logger_, "fail to truncate schema: {}", schema_path_);
      return false;
    }

    auto schema_writer = schema_manager.writer();
    schema_writer.write(&writer_count, sizeof(size_t), 0);
    schema_writer.write(&rows, sizeof(size_t), 1 * sizeof(size_t));
    for (size_t i = 0; i < column_bit_widths.size(); i++) {
      schema_writer.write(&column_bit_widths[i], sizeof(size_t), (2 + i) * sizeof(size_t));
    }
    writer_count_ = writer_count;
    rows_ = rows;
    column_bit_widths_ = column_bit_widths;

    // create data and mask file
    auto create_file = [&]() -> bool {
      auto data_capacity = rows * writer_count *
                           std::accumulate(column_bit_widths.begin(), column_bit_widths.end(), 0) * sizeof(std::byte);
      if (!::mmap_db::truncate(data_path_, data_capacity, true)) {
        quill::error(logger_, "fail to truncate data: {}", data_path_);
        return false;
      }
      auto mask_capacity = rows * writer_count * sizeof(std::byte);
      if (!::mmap_db::truncate(mask_path_, mask_capacity, true)) {
        quill::error(logger_, "fail to truncate mask: {}", mask_path_);
        return false;
      }
      return true;
    };
    if (!create_file()) {
      // remove data_path, mask_path and schema_path
      std::filesystem::remove(data_path_);
      std::filesystem::remove(mask_path_);
      std::filesystem::remove(schema_path_);
      return false;
    }
    return true;
  }

  ArrowWriter writer(size_t writer_id) {
    if (writer_id >= writer_count_) {
      throw std::runtime_error("writer_id out of range");
    }
    return ArrowWriter(data_manager_.writer(), mask_manager_.writer(), writer_id, rows_, column_bit_widths_,
                       writer_count_, logger_);
  }

  std::string info() {
    auto info = std::format("writer_count: {}, rows: {}", writer_count_, rows_);
    for (size_t i = 0; i < column_bit_widths_.size(); i++) {
      info += std::format(", column_bit_widths[{}] = {}", i, column_bit_widths_[i]);
    }
    return info;
  }

 private:
  std::string data_path_;
  std::string mask_path_;
  std::string schema_path_;
  size_t writer_count_ = 0;
  size_t rows_ = 0;
  MmapManager data_manager_;
  MmapManager mask_manager_;
  quill::Logger* logger_;

  std::vector<size_t> column_bit_widths_ = {};
};
}  // namespace mmap_db::arrow

#endif  // MMAP_DB_ARROW_DB_HPP
