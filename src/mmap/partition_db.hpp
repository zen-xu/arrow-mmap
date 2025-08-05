#ifndef MMAP_PARTITION_DB_HPP
#define MMAP_PARTITION_DB_HPP
#pragma once

#include <algorithm>

#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <sys/mman.h>

#include "manager.hpp"

namespace mmap_db {

template <typename Tuple, std::size_t N>
  requires (N < std::tuple_size<Tuple>::value) && ([]<std::size_t... I>(std::index_sequence<I...>) {
             return std::conjunction_v<std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
           }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDBWriter {
 public:
  PartitionDBWriter(MmapWriter data_writer, MmapWriter mask_writer, size_t capacity)
      : data_writer_(data_writer), mask_writer_(mask_writer), capacity_(capacity) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  bool write(const std::tuple_element_t<N, Tuple>& partition_data) {
    auto ret = write(partition_data, index_);
    if (ret) {
      index_++;
    }
    return ret;
  }

  bool write(const std::tuple_element_t<N, Tuple>& partition_data, size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to write: capacity reached");
      return false;
    }

    static constexpr size_t tuple_value = std::tuple_size<Tuple>::value;

    auto data_tuple_array = reinterpret_cast<Tuple*>(data_writer_.mmap_addr());
    std::get<N>(data_tuple_array[index]) = partition_data;

    auto mask_tuple_array = mask_writer_.mmap_addr();
    mask_tuple_array[index * tuple_value + N] = std::byte(0xFF);

    return true;
  }

 private:
  static constexpr size_t tuple_size_ = sizeof(Tuple);

  size_t index_ = 0;
  size_t capacity_ = 0;
  MmapWriter data_writer_;
  MmapWriter mask_writer_;
  quill::Logger* logger_;
};

template <typename Tuple>
  requires ([]<std::size_t... I>(std::index_sequence<I...>) {
    return std::conjunction_v<std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
  }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDBReader {
 public:
  PartitionDBReader(MmapReader data_reader, MmapReader mask_reader, size_t capacity, size_t index = 0)
      : data_reader_(data_reader), mask_reader_(mask_reader), capacity_(capacity), index_(index) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  Tuple* read() {
    auto ret = read(index_);
    if (ret) {
      index_++;
    }
    return ret;
  }

  Tuple* read(size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to read: index out of capacity {} >= {}", index, capacity_);
      return nullptr;
    }

    constexpr size_t mask_size = std::tuple_size<Tuple>::value * sizeof(std::byte);
    auto mask_addr = mask_reader_.read(mask_size, index * mask_size);
    if (mask_addr == nullptr) {
      return nullptr;
    }
    if (!std::all_of(mask_addr, mask_addr + mask_size, [](std::byte b) { return b == std::byte(0xFF); })) {
      return nullptr;
    }
    auto data = data_reader_.read(sizeof(Tuple), sizeof(Tuple) * index);
    if (data == nullptr) {
      return nullptr;
    }

    return reinterpret_cast<Tuple*>(data);
  }

 private:
  size_t capacity_ = 0;
  size_t index_ = 0;
  MmapReader data_reader_;
  MmapReader mask_reader_;
  quill::Logger* logger_;
};

struct PartitionDBConfig {
  int reader_flags = 0;
  int writer_flags = 0;
};

template <typename Tuple>
  requires ([]<std::size_t... I>(std::index_sequence<I...>) {
    return std::conjunction_v<std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
  }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDB {
 public:
  PartitionDB(const std::string& path, PartitionDBConfig config = PartitionDBConfig())
      : path_(path),
        data_path_(std::filesystem::path(path) / "data.mmap"),
        mask_path_(std::filesystem::path(path) / "mask.mmap"),
        data_manager_(data_path_, config.reader_flags, config.writer_flags),
        mask_manager_(mask_path_, config.reader_flags, config.writer_flags) {}

  size_t capacity() const {
    if (!std::filesystem::exists(mask_path_)) {
      return 0;
    }
    int fd = open(mask_path_.c_str(), O_RDONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    struct stat st;
    fstat(fd, &st);
    auto length = static_cast<size_t>(st.st_size);
    close(fd);

    return length / (sizeof(std::byte) * std::tuple_size<Tuple>::value);
  }

  bool truncate_or_create(size_t capacity, bool clear = false) {
    if (clear) {
      std::filesystem::remove(data_path_);
      std::filesystem::remove(mask_path_);
    }

    auto create_file = [&]() -> bool {
      auto data_file_length = capacity * sizeof(Tuple);
      if (!truncate(data_path_, data_file_length, true)) {
        return false;
      }
      auto mask_file_length = capacity * sizeof(std::byte) * std::tuple_size<Tuple>::value;
      if (!truncate(mask_path_, mask_file_length, true)) {
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

  template <std::size_t N>
    requires (N < std::tuple_size<Tuple>::value)
  PartitionDBWriter<Tuple, N> writer() {
    return PartitionDBWriter<Tuple, N>(data_manager_.writer(), mask_manager_.writer(), capacity());
  }

  PartitionDBReader<Tuple> reader() {
    return PartitionDBReader<Tuple>(data_manager_.reader(), mask_manager_.reader(), capacity());
  }

 private:
  std::string path_;
  std::string data_path_;
  std::string mask_path_;
  MmapManager data_manager_;
  MmapManager mask_manager_;
};
}  // namespace mmap_db
#endif
