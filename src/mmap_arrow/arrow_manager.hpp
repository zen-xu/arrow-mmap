#ifndef MMAP_ARROW_ARROW_MANAGER_HPP
#define MMAP_ARROW_ARROW_MANAGER_HPP
#pragma once

#include <fstream>

#include <arrow/api.h>

#include "mmap_arrow/manager.hpp"

namespace mmap_arrow {

struct ArrowMeta {
  size_t writer_count;
  size_t array_length;
  size_t capacity;
  std::shared_ptr<arrow::Schema> schema;

  std::string to_string() const;

  void serialize(std::ofstream& ofs) const;
  void serialize(const std::string& output_file) const;
  static ArrowMeta deserialize(std::ifstream& ifs);
  static ArrowMeta deserialize(const std::string& input_file);
};

class ArrowManager {
 public:
  ArrowManager(const std::string& location, const MmapManagerOptions& options = {});
  ~ArrowManager();

  // disable copy and assign
  ArrowManager(const ArrowManager&) = delete;
  ArrowManager& operator=(const ArrowManager&) = delete;

  // support move
  ArrowManager(ArrowManager&& other) noexcept : impl_(other.impl_) { other.impl_ = nullptr; }

  /**
   * @brief ArrowManager manages Arrow data in mmap format.
   *
   * @param location The directory where mmap files are stored.
   * @param writer_count The number of writers.
   * @param array_length The array length of each RecordBatch.
   * @param schema The schema of the Arrow data.
   * @param capacity The number of RecordBatches (i.e., how many RecordBatches can be stored in total).
   */
  static ArrowManager create(const std::string& location, const size_t writer_count, const size_t array_length,
                             const size_t capacity, const std::shared_ptr<arrow::Schema> schema,
                             const MmapManagerCreateOptions& options = {});

  /**
   * @brief Check if the ArrowManager is ready to use.
   *
   * @param location The directory where mmap files are stored.
   * @return true if the ArrowManager is ready to use, false otherwise.
   */
  static inline bool ready(const std::string& location) noexcept;

  /**
   * @brief Get the meta of the ArrowManager.
   *
   * @return The meta of the ArrowManager.
   */
  const ArrowMeta& meta() const noexcept;

 private:
  class Impl;
  friend class Impl;

  ArrowManager(Impl* impl) : impl_(impl) {}

  Impl* impl_;
};

}  // namespace mmap_arrow

#endif  // MMAP_ARROW_ARROW_MANAGER_HPP
