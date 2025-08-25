#ifndef ARROW_MMAP_MANAGER_HPP
#define ARROW_MMAP_MANAGER_HPP
#pragma once

#include <string>

#include "interface.hpp"

namespace arrow_mmap {

struct MmapManagerOptions {
  int reader_flags = 0;
  int writer_flags = 0;
};

struct MmapManagerCreateOptions : public MmapManagerOptions {
  std::byte fill_with = std::byte(0x00);
};

class MmapManager {
 public:
  MmapManager(const std::string& file, const MmapManagerOptions& options = {});
  static MmapManager create(const std::string& file, size_t length, const MmapManagerCreateOptions& options = {});
  ~MmapManager();

  // disable copy and assign
  MmapManager(const MmapManager&) = delete;
  MmapManager& operator=(const MmapManager&) = delete;

  // support move
  MmapManager(MmapManager&& other) noexcept : impl_(other.impl_) { other.impl_ = nullptr; }

  // since we never unmap the file, so we don't need to use shared_ptr
  IMmapReader* reader() const noexcept;
  IMmapWriter* writer() const noexcept;

 private:
  class Impl;
  friend class Impl;

  MmapManager(Impl* impl) : impl_(impl) {}

  Impl* impl_;
};

}  // namespace arrow_mmap

#endif  // ARROW_MMAP_MANAGER_HPP
