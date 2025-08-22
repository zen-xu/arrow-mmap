#ifndef MMAP_ARROW_MMAP_INTERFACE_HPP
#define MMAP_ARROW_MMAP_INTERFACE_HPP
#pragma once

#include <cstddef>

namespace mmap_arrow {

class IMmapReader {
 public:
  virtual size_t length() const = 0;
  virtual const std::byte* mmap_addr() const = 0;
};

class IMmapWriter {
 public:
  virtual size_t length() const = 0;
  virtual std::byte* mmap_addr() const = 0;
};

}  // namespace mmap_arrow

#endif
