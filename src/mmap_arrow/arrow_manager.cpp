#include "arrow_manager.hpp"

#include <filesystem>
#include <libassert/assert.hpp>
#include <vector>

namespace mmap_arrow {

const std::string get_data_file(const std::string& location) {
  return std::filesystem::path(std::filesystem::absolute(location)) / "data.mmap";
}

const std::string get_bitmap_file(const std::string& location) {
  return std::filesystem::path(std::filesystem::absolute(location)) / "bitmap.mmap";
}

const std::string get_meta_file(const std::string& location) {
  return std::filesystem::path(std::filesystem::absolute(location)) / "meta.bin";
}

class ArrowManager::Impl {
 public:
  Impl(MmapManager&& data_manager, MmapManager&& bitmap_manager, const ArrowMeta meta)
      : data_manager_(std::move(data_manager)),
        bitmap_manager_(std::move(bitmap_manager)),
        meta_(meta),
        writers_(std::vector<std::shared_ptr<ArrowWriter>>(meta.writer_count)) {}

  const std::shared_ptr<ArrowWriter> writer(const size_t id) noexcept {
    if (id >= meta_.writer_count) {
      return nullptr;
    }
    auto writer = writers_[id];
    if (nullptr == writer) {
      writer = std::make_shared<ArrowWriter>(id, meta_, data_manager_.writer(), bitmap_manager_.writer());
      writers_[id] = writer;
    }
    return writer;
  }

 private:
  friend class ArrowManager;

  const MmapManager data_manager_;
  const MmapManager bitmap_manager_;
  const ArrowMeta meta_;
  std::vector<std::shared_ptr<ArrowWriter>> writers_;
};

ArrowManager::ArrowManager(const std::string& location, const MmapManagerOptions& options) {
  ASSERT(ready(location), "ArrowManager is not ready to use");

  auto meta_file = get_meta_file(location);
  auto meta = ArrowMeta::deserialize(meta_file);
  auto data_file = get_data_file(location);
  auto bitmap_file = get_bitmap_file(location);
  auto data_manager = MmapManager(data_file, options);
  auto bitmap_manager = MmapManager(bitmap_file, options);
  impl_ = new Impl(std::move(data_manager), std::move(bitmap_manager), meta);
}

ArrowManager::~ArrowManager() {
  if (impl_) {
    delete impl_;
  }
}

ArrowManager ArrowManager::create(const std::string& location, const size_t writer_count, const size_t array_length,
                                  const size_t capacity, const std::shared_ptr<arrow::Schema> schema,
                                  const MmapManagerCreateOptions& options) {
  if (!std::filesystem::exists(location)) {
    std::filesystem::create_directories(location);
  }

  ASSERT(writer_count > 0, "writer_count must be greater than 0");
  ASSERT(array_length > 0, "array_length must be greater than 0");
  ASSERT(capacity > 0, "capacity must be greater than 0");
  ASSERT(!schema->fields().empty(), "schema must have at least one field");
  ASSERT(writer_count <= array_length, "writer_count must be less than or equal to array_length");

  // init data manager
  auto data_file = get_data_file(location);
  auto data_length = capacity * array_length *
                     std::accumulate(schema->fields().begin(), schema->fields().end(), 0,
                                     [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); });
  auto data_manager = MmapManager::create(data_file, data_length, options);

  // init bitmap manager
  auto bitmap_file = get_bitmap_file(location);
  auto bitmap_length = capacity * writer_count;
  auto bitmap_manager = MmapManager::create(bitmap_file, bitmap_length, options);

  // init meta
  auto meta = ArrowMeta{
      .writer_count = writer_count,
      .array_length = array_length,
      .capacity = capacity,
      .schema = schema,
  };

  // make sure create meta is atomic, which means when meta file is created, the ArrowManager is ready to use
  auto meta_file = get_meta_file(location);
  auto meta_tmp_file = meta_file + ".tmp";
  meta.serialize(meta_tmp_file);
  std::filesystem::rename(meta_tmp_file, meta_file);

  auto impl = new Impl(std::move(data_manager), std::move(bitmap_manager), meta);
  return ArrowManager(impl);
}

inline bool ArrowManager::ready(const std::string& location) noexcept {
  return std::filesystem::exists(get_meta_file(location));
}

const ArrowMeta& ArrowManager::meta() const noexcept { return impl_->meta_; }

const std::shared_ptr<ArrowWriter> ArrowManager::writer(const size_t id) noexcept { return impl_->writer(id); }

}  // namespace mmap_arrow
