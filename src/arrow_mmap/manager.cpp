#include "arrow_mmap/manager.hpp"

#include <cstring>
#include <filesystem>
#include <libassert/assert.hpp>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace arrow_mmap {

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

  MmapReader* reader() {
    if (nullptr == reader_) {
      auto addr = ::mmap(NULL, file_length_, PROT_READ, MAP_PRIVATE | options_.reader_flags, file_fd_, 0);
      ASSERT(addr != MAP_FAILED, std::format("reader failed to mmap file: {}, error: {}", file_, strerror(errno)));
      reader_ = new MmapReader(static_cast<std::byte*>(addr), file_length_);
    }
    return reader_;
  }

  MmapWriter* writer() {
    if (nullptr == writer_) {
      auto addr = ::mmap(NULL, file_length_, PROT_READ | PROT_WRITE, MAP_SHARED | options_.writer_flags, file_fd_, 0);
      ASSERT(addr != MAP_FAILED, std::format("writer failed to mmap file: {}, error: {}", file_, strerror(errno)));
      writer_ = new MmapWriter(static_cast<std::byte*>(addr), file_length_);
    }
    return writer_;
  }

 private:
  friend class MmapManager;

  const std::string file_;
  const MmapManagerOptions options_;
  const int file_length_;
  const int file_fd_;

  MmapReader* reader_ = nullptr;
  MmapWriter* writer_ = nullptr;
};

MmapManager::MmapManager(const std::string& file, const MmapManagerOptions& options) {
  // try to open file
  int fd = open(file.c_str(), O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  ASSERT(fd != -1, std::format("failed to open file: {}, error: {}", file, strerror(errno)));

  size_t length = get_fd_length(fd);
  ASSERT(length > 0, std::format("file {} is empty", file));
  impl_ = new MmapManager::Impl(file, fd, length, options);
}

MmapManager MmapManager::create(const std::string& file, size_t length, const MmapManagerCreateOptions& options) {
  ASSERT(length > 0, std::format("can't create mmap file with 0 length, file: {}", file));

  std::filesystem::path file_dir = std::filesystem::path(std::filesystem::absolute(file)).parent_path();
  if (!std::filesystem::exists(file_dir)) {
    std::filesystem::create_directories(file_dir);
  }

  int fd = open(file.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  ASSERT(fd != -1, std::format("failed to open file: {}, error: {}", file, strerror(errno)));

  ASSERT(ftruncate(fd, length) != -1, std::format("failed to truncate file: {}, error: {}", file, strerror(errno)));

  // filling the file content with `options.fill_with`
  void* addr = ::mmap(nullptr, length, PROT_WRITE, MAP_SHARED, fd, 0);
  ASSERT(addr != MAP_FAILED, std::format("failed to mmap file: {}, error: {}", file, strerror(errno)));
  std::memset(addr, static_cast<int>(options.fill_with), length);
  munmap(addr, length);

  return MmapManager(new MmapManager::Impl(file, fd, length, options));
}

MmapManager::~MmapManager() {
  if (impl_) {
    delete impl_;
  }
}

IMmapReader* MmapManager::reader() const noexcept { return impl_->reader(); }
IMmapWriter* MmapManager::writer() const noexcept { return impl_->writer(); }
}  // namespace arrow_mmap
