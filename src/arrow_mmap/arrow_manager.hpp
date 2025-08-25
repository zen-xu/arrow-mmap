#ifndef ARROW_MMAP_ARROW_MANAGER_HPP
#define ARROW_MMAP_ARROW_MANAGER_HPP
#pragma once

#include <arrow/api.h>

#include "arrow_mmap/arrow_meta.hpp"
#include "arrow_mmap/arrow_reader.hpp"
#include "arrow_mmap/arrow_writer.hpp"
#include "arrow_mmap/manager.hpp"

namespace arrow_mmap {

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

  /**
   * @brief Get the ArrowWriter of the ArrowManager.
   *
   * @param id The id of the ArrowWriter.
   * @return The ArrowWriter of the ArrowManager.
   */
  const std::shared_ptr<ArrowWriter> writer(const size_t id) noexcept;

  /**
   * @brief Get the ArrowReader of the ArrowManager.
   *
   * @return The ArrowReader of the ArrowManager.
   */
  const std::shared_ptr<ArrowReader> reader() noexcept;

 private:
  class Impl;
  friend class Impl;

  ArrowManager(Impl* impl) : impl_(impl) {}

  Impl* impl_;
};

}  // namespace arrow_mmap

#endif  // ARROW_MMAP_ARROW_MANAGER_HPP
