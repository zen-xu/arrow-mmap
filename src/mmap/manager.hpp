#ifndef MMAP_MANAGER_HPP
#define MMAP_MANAGER_HPP
#pragma once

#include <cstddef>
#include <filesystem>
#include <string>

#include <fcntl.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mmap_db {

inline size_t get_length(int fd) {
  struct stat st;
  fstat(fd, &st);
  return st.st_size;
}

inline bool truncate(const std::string &file, size_t length,
                     bool fill_zero = false) {
  quill::Logger *logger = quill::Frontend::create_or_get_logger("default");

  std::filesystem::path file_dir = std::filesystem::path(file).parent_path();

  if (!std::filesystem::exists(file_dir)) {
    std::filesystem::create_directories(file_dir);
  }

  if (length == 0) {
    quill::error(logger, "can't truncate to 0 length");
    return false;
  }

  const std::string truncate_file =
      std::filesystem::exists(file) ? file : file + ".tmp";
  int oflag =
      std::filesystem::exists(file) ? O_RDWR : O_RDWR | O_CREAT | O_TRUNC;
  int fd =
      open(truncate_file.c_str(), oflag, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (-1 == fd) {
    quill::error(logger, "failed to open {}: {}", truncate_file,
                 strerror(errno));
    return false;
  }

  if (-1 == ftruncate(fd, length)) {
    quill::error(logger, "fail to truncate {}: {}", truncate_file,
                 strerror(errno));
    close(fd);
    return false;
  }

  int origin_length = get_length(fd);
  if (fill_zero && origin_length < length) {
    void *addr =
        ::mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
      quill::error(logger, "failed to mmap {}: {}", truncate_file,
                   strerror(errno));
      close(fd);
      return false;
    }
    std::memset(static_cast<char *>(addr) + origin_length, 0,
                length - origin_length);
    munmap(addr, length);
  }

  if (-1 == close(fd)) {
    quill::error(logger, "failed to close {}: {}", truncate_file,
                 strerror(errno));
    return false;
  }

  if (truncate_file != file) {
    if (-1 == rename(truncate_file.c_str(), file.c_str())) {
      quill::error(logger, "failed to move {} to {}: {}", truncate_file, file,
                   strerror(errno));
      return false;
    }
  }
  return true;
}

class MmapWriter {
public:
  MmapWriter(int fd, void *mmap_addr, size_t length)
      : fd_(fd), mmap_addr_(mmap_addr), length_(length) {}
  MmapWriter() = default;

  size_t length() const { return length_; }

  bool write(const void *buf, size_t buf_n, size_t mmap_offset) {
    if (-1 == fd_) {
      return false;
    }
    if (mmap_offset + buf_n > length_) {
      return false;
    }
    std::memcpy(static_cast<char *>(mmap_addr_) + mmap_offset, buf, buf_n);
    return true;
  }

  void *mmap_addr() const { return mmap_addr_; }

private:
  int length_ = 0;
  int fd_ = -1;
  void *mmap_addr_ = nullptr;
};

class MmapReader {
public:
  MmapReader(int fd, void *mmap_addr, size_t length)
      : fd_(fd), mmap_addr_(mmap_addr), length_(length) {}
  MmapReader() = default;

  size_t length() const { return length_; }

  void *mmap_addr() const { return mmap_addr_; }

  void *read(size_t data_size, size_t mmap_offset) {
    if (mmap_offset + data_size > length_) {
      return nullptr;
    }
    return static_cast<char *>(mmap_addr_) + mmap_offset;
  }

  bool read(void *buf, size_t buf_n, size_t mmap_offset) {
    auto src_ptr = read(buf_n, mmap_offset);
    if (src_ptr == nullptr) {
      return false;
    }
    std::memcpy(buf, src_ptr, buf_n);
    return true;
  }

private:
  int length_ = 0;
  int fd_ = -1;
  void *mmap_addr_ = nullptr;
};

class MmapManager {
public:
  explicit MmapManager(const std::string &file, int reader_flags = 0,
                       int writer_flags = 0)
      : file_(file), reader_flags_(reader_flags), writer_flags_(writer_flags) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  MmapWriter writer() {
    MmapWriter writer;
    if (!init_fd()) {
      return writer;
    }

    if (mmap_writer_addr_ == nullptr) {
      auto addr = ::mmap(NULL, length_, PROT_READ | PROT_WRITE,
                         MAP_SHARED | writer_flags_, fd_, 0);
      if (addr == MAP_FAILED) {
        quill::error(logger_, "failed to mmap {}: {}", file_, strerror(errno));
        return writer;
      }
      mmap_writer_addr_ = addr;
    }

    return MmapWriter(fd_, mmap_writer_addr_, length_);
  }

  MmapReader reader(int mmap_flags = 0) {
    auto reader = MmapReader();

    if (!init_fd()) {
      return reader;
    }

    if (mmap_reader_addr_ == nullptr) {
      auto addr =
          ::mmap(NULL, length_, PROT_READ, MAP_PRIVATE | reader_flags_, fd_, 0);
      if (addr == MAP_FAILED) {
        quill::error(logger_, "failed to mmap {}: {}", file_, strerror(errno));
        return reader;
      }
      mmap_reader_addr_ = addr;
    }

    return MmapReader(fd_, mmap_reader_addr_, length_);
  }

  ~MmapManager() {
    if (mmap_reader_addr_ != nullptr) {
      quill::debug(logger_, "unmap reader {}", file_);
      munmap(mmap_reader_addr_, length_);
    }
    if (mmap_writer_addr_ != nullptr) {
      quill::debug(logger_, "unmap writer {}", file_);
      munmap(mmap_writer_addr_, length_);
    }
    if (fd_ != -1) {
      quill::debug(logger_, "close fd");
      close(fd_);
    }
  }

private:
  bool init_fd() {
    if (-1 == fd_) {
      int fd = open(file_.c_str(), O_RDWR | O_CREAT,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      if (fd == -1) {
        quill::error(logger_, "failed to open {}: {}", file_, strerror(errno));
        close(fd);
        return false;
      }

      size_t length = get_length(fd);
      if (length == 0) {
        close(fd);
        return false;
      }

      fd_ = fd;
      length_ = length;
    }
    return true;
  }

  std::string file_;
  quill::Logger *logger_ = nullptr;
  int fd_ = -1;
  size_t length_ = 0;
  int reader_flags_ = 0;
  int writer_flags_ = 0;
  void *mmap_reader_addr_ = nullptr;
  void *mmap_writer_addr_ = nullptr;
};
} // namespace mmap_db
#endif // MMAP_MANAGER_HPP