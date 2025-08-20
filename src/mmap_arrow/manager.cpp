#include "manager.hpp"

#include <cstring>
#include <filesystem>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mmap_arrow {

size_t get_fd_length(int fd) {
  struct stat st;
  fstat(fd, &st);
  return st.st_size;
}

class MmapReader : public IMmapReader {
 public:
  MmapReader(std::byte* addr, size_t length) : addr_(addr), length_(length) {}

  inline size_t length() const override { return length_; }
  inline const std::byte* mmap_addr() const override { return addr_; }

 private:
  const size_t length_;
  std::byte* addr_;
};

class MmapWriter : public IMmapWriter {
 public:
  MmapWriter(std::byte* addr, size_t length) : addr_(addr), length_(length) {}

  inline size_t length() const override { return length_; }
  inline std::byte* mmap_addr() const override { return addr_; }

 private:
  const size_t length_;
  std::byte* addr_;
};

class MmapManager::Impl {
 public:
  Impl(const std::string& file, int file_fd, size_t file_length, const MmapManagerOptions& options)
      : file_(file), file_fd_(file_fd), file_length_(file_length), options_(options) {}

  Impl(const std::string& file, const MmapManagerOptions& options) : file_(file), options_(options) {
    // try to open file
    int fd = open(file_.c_str(), O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
      throw std::filesystem::filesystem_error("failed to open file", file_,
                                              std::error_code(errno, std::generic_category()));
    }

    size_t length = get_fd_length(fd);
    if (length == 0) {
      close(fd);
      throw std::runtime_error(std::format("file {} is empty", file_));
    }

    file_fd_ = fd;
    file_length_ = length;
  }

  std::shared_ptr<MmapReader> reader() {
    if (file_fd_ == -1) {
      throw std::runtime_error("MmapManager is not initialized");
    }

    if (nullptr == reader_) {
      auto addr = ::mmap(NULL, file_length_, PROT_READ, MAP_SHARED | options_.reader_flags, file_fd_, 0);
      if (addr == MAP_FAILED) {
        throw std::filesystem::filesystem_error("reader failed to mmap file", file_,
                                                std::error_code(errno, std::generic_category()));
      }
      reader_ = std::make_shared<MmapReader>(static_cast<std::byte*>(addr), file_length_);
    }
    return reader_;
  }

  std::shared_ptr<MmapWriter> writer() {
    if (file_fd_ == -1) {
      throw std::runtime_error("MmapManager is not initialized");
    }

    if (nullptr == writer_) {
      auto addr = ::mmap(NULL, file_length_, PROT_READ | PROT_WRITE, MAP_SHARED | options_.writer_flags, file_fd_, 0);
      if (addr == MAP_FAILED) {
        throw std::filesystem::filesystem_error("writer failed to mmap file", file_,
                                                std::error_code(errno, std::generic_category()));
      }
      writer_ = std::make_shared<MmapWriter>(static_cast<std::byte*>(addr), file_length_);
    }
    return writer_;
  }

 private:
  const std::string file_;
  const MmapManagerOptions options_;

  int file_length_ = 0;
  int file_fd_ = -1;
  std::shared_ptr<MmapReader> reader_ = nullptr;
  std::shared_ptr<MmapWriter> writer_ = nullptr;
};

MmapManager::MmapManager(const std::string& file, const MmapManagerOptions& options)
    : impl_(new MmapManager::Impl(file, options)) {}

MmapManager MmapManager::create(const std::string& file, size_t length, const MmapManagerCreateOptions& options) {
  if (length == 0) {
    throw std::runtime_error(std::format("can't create mmap file with 0 length, file: {}", file));
  }

  std::filesystem::path file_dir = std::filesystem::path(std::filesystem::absolute(file)).parent_path();
  if (!std::filesystem::exists(file_dir)) {
    std::filesystem::create_directories(file_dir);
  }

  int fd = open(file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd == -1) {
    throw std::filesystem::filesystem_error("failed to open file", file,
                                            std::error_code(errno, std::generic_category()));
  }

  if (ftruncate(fd, length) == -1) {
    throw std::filesystem::filesystem_error("failed to truncate file", file,
                                            std::error_code(errno, std::generic_category()));
  }

  // filling the file content with `options.fill_with`
  void* addr = ::mmap(nullptr, length, PROT_WRITE, MAP_PRIVATE, fd, 0);
  if (addr == MAP_FAILED) {
    close(fd);
    throw std::filesystem::filesystem_error("failed to mmap file", file,
                                            std::error_code(errno, std::generic_category()));
  }
  std::memset(addr, static_cast<int>(options.fill_with), length);
  munmap(addr, length);

  return MmapManager(new MmapManager::Impl(file, fd, length, options));
}

MmapManager::~MmapManager() { delete impl_; }

std::shared_ptr<IMmapReader> MmapManager::reader() const { return impl_->reader(); }
std::shared_ptr<IMmapWriter> MmapManager::writer() const { return impl_->writer(); }
}  // namespace mmap_arrow
