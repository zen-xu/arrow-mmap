#ifndef MMAP_DYN_PARTITION_DB_HPP
#define MMAP_DYN_PARTITION_DB_HPP
#pragma once

#include <quill/Frontend.h>
#include <quill/LogFunctions.h>

#include "manager.hpp"

namespace mmap_db {

struct DynPartitionDBConfig {
  int reader_flags = 0;
  int writer_flags = 0;
};

class DynPartitionDBWriter {
 public:
  DynPartitionDBWriter(MmapWriter data_writer, MmapWriter mask_writer, size_t partition, size_t partition_size,
                       size_t partition_offset, size_t partition_count, size_t chunk_size, size_t capacity,
                       quill::Logger* logger)
      : data_writer_(data_writer),
        mask_writer_(mask_writer),
        partition_(partition),
        partition_size_(partition_size),
        partition_offset_(partition_offset),
        partition_count_(partition_count),
        chunk_size_(chunk_size),
        capacity_(capacity),
        logger_(logger) {}

  inline bool write(const void* partition_data) {
    auto ret = write(partition_data, index_);
    if (ret) {
      index_++;
    }
    return ret;
  }

  inline bool write(const void* partition_data, size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to write: index out of capacity {} >= {}", index, capacity_);
      return false;
    }
    if (!data_writer_.write(partition_data, partition_size_, index * chunk_size_ + partition_offset_)) {
      return false;
    }
    auto mask_addr = mask_writer_.mmap_addr();
    mask_addr[index * partition_count_ + partition_] = std::byte(0xFF);
    return true;
  }

  std::byte* addr() { return data_writer_.mmap_addr(); }

 private:
  size_t index_ = 0;
  size_t capacity_ = 0;
  size_t partition_;
  size_t partition_size_;
  size_t partition_offset_;
  size_t partition_count_;
  size_t chunk_size_;
  MmapWriter data_writer_;
  MmapWriter mask_writer_;
  quill::Logger* logger_;
};

class DynPartitionDBReader {
 public:
  DynPartitionDBReader(MmapReader data_reader, MmapReader mask_reader, size_t chunk_size, size_t capacity,
                       size_t partition_count_, quill::Logger* logger)
      : data_reader_(data_reader),
        mask_reader_(mask_reader),
        chunk_size_(chunk_size),
        capacity_(capacity),
        partition_count_(partition_count_),
        logger_(logger) {}

  inline std::byte* read() {
    auto ret = read(index_);
    if (ret) {
      index_++;
    }
    return ret;
  }

  inline std::byte* read(size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to read: index out of capacity {} >= {}", index, capacity_);
      return nullptr;
    }
    auto mask_chunk_size = sizeof(std::byte) * partition_count_;
    auto mask_addr = mask_reader_.read(mask_chunk_size, mask_chunk_size * index);
    if (mask_addr == nullptr) {
      return nullptr;
    }
    if (!std::all_of(mask_addr, mask_addr + mask_chunk_size, [](std::byte b) { return b == std::byte(0xFF); })) {
      return nullptr;
    }
    return data_reader_.read(chunk_size_, index * chunk_size_);
  }

 private:
  size_t capacity_ = 0;
  size_t index_ = 0;
  size_t chunk_size_;
  size_t partition_count_;
  MmapReader data_reader_;
  MmapReader mask_reader_;
  quill::Logger* logger_;
};

class DynPartitionDB {
 public:
  DynPartitionDB(const std::string& path, DynPartitionDBConfig config = DynPartitionDBConfig())
      : path_(path),
        data_path_(std::filesystem::path(path) / "data.mmap"),
        mask_path_(std::filesystem::path(path) / "mask.mmap"),
        schema_path_(std::filesystem::path(path) / "schema.mmap"),
        data_manager_(data_path_, config.reader_flags, config.writer_flags),
        mask_manager_(mask_path_, config.reader_flags, config.writer_flags),
        logger_(quill::Frontend::create_or_get_logger("default")) {
    if (std::filesystem::exists(schema_path_)) {
      auto schema_manager = MmapManager(schema_path_);
      auto schema_reader = schema_manager.reader();

      auto buf = reinterpret_cast<size_t*>(schema_reader.read(schema_reader.length(), 0));
      if (nullptr != buf) {
        for (size_t i = 0; i < schema_reader.length() / sizeof(size_t); i++) {
          partition_sizes_.push_back(buf[i]);
        }
      } else {
        quill::error(logger_, "failed to read schema");
      }
    }
  }

  std::vector<size_t> partition_sizes() const { return partition_sizes_; }

  size_t capacity() const {
    if (!std::filesystem::exists(mask_path_)) {
      return 0;
    }
    int fd = open(mask_path_.c_str(), O_RDONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    struct stat st;
    fstat(fd, &st);
    auto length = static_cast<size_t>(st.st_size);
    close(fd);

    return length / (sizeof(std::byte) * partition_sizes_.size());
  }

  bool create(size_t capacity, std::vector<size_t> partition_sizes) {
    if (partition_sizes.empty()) {
      quill::error(logger_, "fail to create: partition_sizes can't be empty");
      return false;
    }
    if (capacity == 0) {
      quill::error(logger_, "fail to create: capacity can't be 0");
      return false;
    }

    auto schema_manager = MmapManager(schema_path_);
    std::filesystem::remove(schema_path_);
    if (!::mmap_db::truncate(schema_path_, sizeof(size_t) * partition_sizes.size(), true)) {
      quill::error(logger_, "fail to truncate schema: {}", schema_path_);
      return false;
    }
    auto schema_writer = schema_manager.writer();
    for (size_t i = 0; i < partition_sizes.size(); i++) {
      schema_writer.write(&partition_sizes[i], sizeof(size_t), i * sizeof(size_t));
    }

    partition_sizes_ = partition_sizes;
    return truncate(capacity, true);
  }

  bool truncate(size_t capacity, bool clear = false) {
    if (partition_sizes_.empty()) {
      quill::error(logger_, "fail to truncate: partition_sizes is empty");
      return false;
    }

    if (clear) {
      std::filesystem::remove(data_path_);
      std::filesystem::remove(mask_path_);
    }

    size_t chunk_size = 0;
    for (auto sz : partition_sizes_) {
      chunk_size += sz;
    }

    auto create_file = [&]() -> bool {
      auto data_file_length = capacity * chunk_size;
      if (!::mmap_db::truncate(data_path_, data_file_length, true)) {
        quill::error(logger_, "fail to truncate data: {}", data_path_);
        return false;
      }
      auto mask_file_length = capacity * sizeof(std::byte) * partition_sizes_.size();
      if (!::mmap_db::truncate(mask_path_, mask_file_length, true)) {
        quill::error(logger_, "fail to truncate mask: {}", mask_path_);
        return false;
      }
      return true;
    };
    if (!create_file()) {
      // remove data_path and mask_path
      std::filesystem::remove(data_path_);
      std::filesystem::remove(mask_path_);
      return false;
    }
    return true;
  }

  DynPartitionDBWriter writer(size_t partition) {
    auto partition_offset = 0;
    for (size_t i = 0; i < partition; i++) {
      partition_offset += partition_sizes_[i];
    }
    auto chunk_size = 0;
    for (size_t i = 0; i < partition_sizes_.size(); i++) {
      chunk_size += partition_sizes_[i];
    }
    return DynPartitionDBWriter(data_manager_.writer(), mask_manager_.writer(), partition, partition_sizes_[partition],
                                partition_offset, partition_sizes_.size(), chunk_size, capacity(), logger_);
  }

  DynPartitionDBReader reader() {
    auto chunk_size = 0;
    for (size_t i = 0; i < partition_sizes_.size(); i++) {
      chunk_size += partition_sizes_[i];
    }
    return DynPartitionDBReader(data_manager_.reader(), mask_manager_.reader(), chunk_size, capacity(),
                                partition_sizes_.size(), logger_);
  }

 private:
  std::string path_;
  std::string data_path_;
  std::string mask_path_;
  std::string schema_path_;
  MmapManager data_manager_;
  MmapManager mask_manager_;
  quill::Logger* logger_;

  std::vector<size_t> partition_sizes_ = {};
};
}  // namespace mmap_db

#endif  // MMAP_DYN_PARTITION_DB_HPP
