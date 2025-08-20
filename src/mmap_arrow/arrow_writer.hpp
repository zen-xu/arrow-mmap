#ifndef MMAP_ARROW_ARROW_WRITER_HPP
#define MMAP_ARROW_ARROW_WRITER_HPP
#pragma once

#include "mmap_arrow/arrow_manager.hpp"

namespace mmap_arrow {

class ArrowWriter {
 public:
  ArrowWriter(const size_t id, const ArrowMeta meta, const IMmapWriter* writer)
      : id(id), meta_(meta), writer_(writer), array_length([id, meta]() {
          if (id < meta.writer_count - 1) {
            return meta.array_length / meta.writer_count;
          } else {
            return meta.array_length - meta.array_length / meta.writer_count * meta.writer_count;
          }
        }()) {}

  const size_t array_length;
  const size_t id;

 private:
  const ArrowMeta meta_;
  const IMmapWriter* writer_;
};

}  // namespace mmap_arrow
#endif
