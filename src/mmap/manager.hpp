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

struct MmapFD {
  int fd;
  size_t length;
  void *mmap_addr;
  quill::Logger *logger_;

  MmapFD(int fd, void *mmap_addr, quill::Logger *logger)
      : fd(fd), mmap_addr(mmap_addr), length(get_length(fd)), logger_(logger) {}

  ~MmapFD() {
    if (mmap_addr != MAP_FAILED) {
      quill::info(logger_, "munmap {} length: {}", fd, length);
      munmap(mmap_addr, length);
      if (fd != -1) {
        close(fd);
        fd = -1;
      }
    }
  }
};

class MmapWriter {
public:
  size_t length() const { return fd_->length; }

  bool write(const void *buf, size_t buf_n, size_t mmap_offset) {
    if (mmap_offset + buf_n > fd_->length) {
      return false;
    }
    std::memcpy(static_cast<char *>(fd_->mmap_addr) + mmap_offset, buf, buf_n);
    return true;
  }

  void *mmap_addr() const { return fd_->mmap_addr; }

private:
  std::shared_ptr<MmapFD> fd_ = nullptr;

  friend class MmapManager;
  MmapWriter() : fd_(nullptr) {}
};

class MmapReader {
public:
  size_t length() const { return fd_->length; }

  void *mmap_addr() const { return fd_->mmap_addr; }

  void *read(size_t data_size, size_t mmap_offset) {
    if (mmap_offset + data_size > fd_->length) {
      return nullptr;
    }
    return static_cast<char *>(fd_->mmap_addr) + mmap_offset;
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
  std::shared_ptr<MmapFD> fd_ = nullptr;

  friend class MmapManager;
  explicit MmapReader() : fd_(nullptr) {}
};

class MmapManager {
public:
  explicit MmapManager(const std::string &file) : file_(file) {
    logger_ = quill::Frontend::create_or_get_logger("default");
  }

  bool truncate(size_t length, bool fill_zero = false) {
    std::filesystem::path file_dir = std::filesystem::path(file_).parent_path();
    if (!std::filesystem::exists(file_dir)) {
      std::filesystem::create_directories(file_dir);
    }

    if (length == 0) {
      quill::error(logger_, "can't truncate to 0 length");
      return false;
    }

    const std::string truncate_file =
        std::filesystem::exists(file_) ? file_ : file_ + ".tmp";
    int oflag =
        std::filesystem::exists(file_) ? O_RDWR : O_RDWR | O_CREAT | O_TRUNC;
    int fd = open(truncate_file.c_str(), oflag,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
      quill::error(logger_, "failed to open {}: {}", truncate_file,
                   strerror(errno));
      return false;
    }

    if (-1 == ftruncate(fd, length)) {
      quill::error(logger_, "fail to truncate {}: {}", truncate_file,
                   strerror(errno));
      close(fd);
      return false;
    }

    int origin_length = get_length(fd);
    if (fill_zero && origin_length < length) {
      void *addr =
          ::mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if (addr == MAP_FAILED) {
        quill::error(logger_, "failed to mmap {}: {}", truncate_file,
                     strerror(errno));
        close(fd);
        return false;
      }
      std::memset(static_cast<char *>(addr) + origin_length, 0,
                  length - origin_length);
      munmap(addr, length);
    }

    if (-1 == close(fd)) {
      quill::error(logger_, "failed to close {}: {}", truncate_file,
                   strerror(errno));
      return false;
    }

    if (truncate_file != file_) {
      if (-1 == rename(truncate_file.c_str(), file_.c_str())) {
        quill::error(logger_, "failed to move {} to {}: {}", truncate_file,
                     file_, strerror(errno));
        return false;
      }
    }
    return true;
  }

  MmapWriter writer(int mmap_flags = 0) {
    auto writer = MmapWriter();
    if (writer_fd_ == nullptr) {
      int fd = open(file_.c_str(), O_RDWR | O_CREAT,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
      if (fd == -1) {
        quill::error(logger_, "failed to open {}: {}", file_, strerror(errno));
        return writer;
      }

      size_t length = get_length(fd);
      if (length == 0) {
        close(fd);
        return writer;
      }

      auto addr = ::mmap(NULL, length, PROT_READ | PROT_WRITE,
                         MAP_SHARED | mmap_flags, fd, 0);
      if (addr == MAP_FAILED) {
        quill::error(logger_, "failed to mmap {}: {}", file_, strerror(errno));
        close(fd);
        return writer;
      }

      writer_fd_ = std::make_shared<MmapFD>(fd, addr, logger_);
      quill::info(logger_, "init writer fd: {}", fd);
    }
    writer.fd_ = writer_fd_;
    return writer;
  }

  MmapReader reader(int mmap_flags = 0) {
    auto reader = MmapReader();
    if (reader_fd_ == nullptr) {
      int fd =
          open(file_.c_str(), O_RDONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

      if (-1 == fd) {
        quill::error(logger_, "failed to open {}: {}", file_, strerror(errno));
        return reader;
      }
      size_t length = get_length(fd);
      if (length == 0) {
        close(fd);
        return reader;
      }

      auto addr =
          ::mmap(NULL, length, PROT_READ, MAP_PRIVATE | mmap_flags, fd, 0);
      if (MAP_FAILED == addr) {
        quill::error(logger_, "failed to mmap {}: {}", file_, strerror(errno));
        close(fd);
        return reader;
      }

      reader_fd_ = std::make_shared<MmapFD>(fd, addr, logger_);
      quill::info(logger_, "init reader fd: {}", fd);
    }
    reader.fd_ = reader_fd_;
    return reader;
  }

private:
  std::string file_;
  quill::Logger *logger_ = nullptr;
  std::shared_ptr<MmapFD> reader_fd_ = nullptr;
  std::shared_ptr<MmapFD> writer_fd_ = nullptr;
};
} // namespace mmap_db
#endif // MMAP_MANAGER_HPP