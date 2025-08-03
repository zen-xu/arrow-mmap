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
  requires(N < std::tuple_size<Tuple>::value) &&
          ([]<std::size_t... I>(std::index_sequence<I...>) {
            return std::conjunction_v<
                std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
          }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDBWriter {
public:
  PartitionDBWriter(std::string data_path, std::string mask_path,
                    size_t capacity)
      : data_writer_(MmapManager(data_path).writer()),
        mask_writer_(MmapManager(mask_path).writer()), capacity_(capacity) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  bool write(const std::tuple_element_t<N, Tuple> &data) {
    if (index_ >= capacity_) {
      quill::error(logger_, "failed to write: capacity reached");
      return false;
    }

    auto data_tuple_array = reinterpret_cast<Tuple *>(data_writer_.mmap_addr());
    std::get<N>(data_tuple_array[index_]) = data;

    auto mask_tuple_array = reinterpret_cast<bool *>(mask_writer_.mmap_addr());
    mask_tuple_array[index_ * tuple_count_ + N] = true;

    index_++;
    return true;
  }

private:
  static constexpr size_t tuple_size_ = sizeof(Tuple);
  static constexpr size_t tuple_count_ = std::tuple_size<Tuple>::value;

  size_t index_ = 0;
  size_t capacity_ = 0;
  MmapWriter data_writer_;
  MmapWriter mask_writer_;
  quill::Logger *logger_;
};

template <typename Tuple>
  requires([]<std::size_t... I>(std::index_sequence<I...>) {
    return std::conjunction_v<
        std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
  }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDBReader {
public:
  PartitionDBReader(std::string data_path, std::string mask_path,
                    size_t capacity)
      : data_reader_(MmapManager(data_path).reader()),
        mask_reader_(MmapManager(mask_path).reader()), capacity_(capacity) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  Tuple *read(size_t index) {
    if (index >= capacity_) {
      quill::error(logger_, "failed to read: index out of capacity {} >= {}",
                   index, capacity_);
      return nullptr;
    }

    if (!mask_reader_.read(&mask_buffer_, sizeof(mask_buffer_),
                           index * sizeof(mask_buffer_))) {
      return nullptr;
    }

    if (!std::all_of(std::begin(mask_buffer_), std::end(mask_buffer_),
                     [](bool b) { return b; })) {
      quill::error(
          logger_, "failed to read: mask is not all true: {}",
          std::string(std::begin(mask_buffer_), std::end(mask_buffer_)));
      return nullptr;
    }

    auto data = data_reader_.read(sizeof(Tuple), sizeof(Tuple) * index);
    if (data == nullptr) {
      return nullptr;
    }

    return reinterpret_cast<Tuple *>(data);
  }

  std::string mask_buffer_string() {
    auto mask_addr = mask_reader_.mmap_addr();
    auto mask_size = mask_reader_.length();
    auto mask_count = mask_size / sizeof(bool);
    auto mask_ptr = reinterpret_cast<bool *>(mask_addr);
    return std::string(mask_ptr, mask_ptr + mask_count);
  }

private:
  size_t capacity_ = 0;
  bool mask_buffer_[std::tuple_size<Tuple>::value] = {false};
  MmapReader data_reader_;
  MmapReader mask_reader_;
  quill::Logger *logger_;
};

template <typename Tuple>
  requires([]<std::size_t... I>(std::index_sequence<I...>) {
    return std::conjunction_v<
        std::is_standard_layout<std::tuple_element_t<I, Tuple>>...>;
  }(std::make_index_sequence<std::tuple_size<Tuple>::value>{}))
class PartitionDB {
public:
  PartitionDB(const std::string &path)
      : path_(path), data_path_(std::filesystem::path(path) / "data.mmap"),
        mask_path_(std::filesystem::path(path) / "mask.mmap") {}

  size_t capacity() const {
    if (!std::filesystem::exists(mask_path_)) {
      return 0;
    }
    int fd = open(mask_path_.c_str(), O_RDONLY,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    struct stat st;
    fstat(fd, &st);
    auto length = static_cast<size_t>(st.st_size);
    close(fd);

    return length / (sizeof(bool) * std::tuple_size<Tuple>::value);
  }

  bool create(size_t capacity) {
    auto create_file = [&]() -> bool {
      auto data_manager = MmapManager(data_path_);
      auto data_file_length = capacity * sizeof(Tuple);
      if (!data_manager.truncate(data_file_length, true)) {
        return false;
      }
      auto mask_manager = MmapManager(mask_path_);
      auto mask_file_length =
          capacity * sizeof(bool) * std::tuple_size<Tuple>::value;
      if (!mask_manager.truncate(mask_file_length, true)) {
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
    requires(N < std::tuple_size<Tuple>::value)
  PartitionDBWriter<Tuple, N> writer() {
    return PartitionDBWriter<Tuple, N>(data_path_, mask_path_, capacity());
  }

  PartitionDBReader<Tuple> reader() {
    return PartitionDBReader<Tuple>(data_path_, mask_path_, capacity());
  }

private:
  std::string path_;
  std::string data_path_;
  std::string mask_path_;
};
} // namespace mmap_db
#endif