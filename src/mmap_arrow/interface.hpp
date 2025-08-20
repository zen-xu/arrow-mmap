#ifndef MMAP_ARROW_MMAP_INTERFACE_HPP
#define MMAP_ARROW_MMAP_INTERFACE_HPP
#pragma once

#include <cstddef>

namespace mmap_arrow {

class IMmapReader {
 public:
  virtual inline size_t length() const = 0;
  virtual inline const std::byte* mmap_addr() const = 0;
};

class IMmapWriter {
 public:
  virtual inline size_t length() const = 0;
  virtual inline std::byte* mmap_addr() const = 0;
};

}  // namespace mmap_arrow

#endif
