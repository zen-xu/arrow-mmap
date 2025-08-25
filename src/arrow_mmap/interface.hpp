#ifndef ARROW_MMAP_INTERFACE_HPP
#define ARROW_MMAP_INTERFACE_HPP
#pragma once

#include <cstddef>

namespace arrow_mmap {

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

}  // namespace arrow_mmap

#endif  // ARROW_MMAP_INTERFACE_HPP
