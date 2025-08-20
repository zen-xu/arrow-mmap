#include "arrow_manager.hpp"

#include <filesystem>
#include <fstream>
#include <vector>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace mmap_arrow {

std::string ArrowMeta::to_string() const {
  return std::format("writer_count: {}\narray_length: {}\ncapacity: {}\nschema:\n{}", writer_count, array_length,
                     capacity, [&] {
                       std::string schema_str = schema->ToString();
                       std::string indented;
                       size_t pos = 0, prev = 0;
                       while ((pos = schema_str.find('\n', prev)) != std::string::npos) {
                         indented += "  " + schema_str.substr(prev, pos - prev + 1);
                         prev = pos + 1;
                       }
                       if (prev < schema_str.size()) {
                         indented += "  " + schema_str.substr(prev);
                       }
                       return indented;
                     }());
}

void ArrowMeta::serialize(std::ofstream& ofs) const {
  auto schema_buffer = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
  ofs.write(reinterpret_cast<const char*>(&writer_count), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(&array_length), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(&capacity), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(schema_buffer->data()), schema_buffer->size());
}

void ArrowMeta::serialize(const std::string& output_file) const {
  std::ofstream ofs(output_file, std::ios::binary);
  serialize(ofs);
  ofs.close();
}

ArrowMeta ArrowMeta::deserialize(std::ifstream& ifs) {
  ArrowMeta meta;
  ifs.read(reinterpret_cast<char*>(&meta.writer_count), sizeof(size_t));
  ifs.read(reinterpret_cast<char*>(&meta.array_length), sizeof(size_t));
  ifs.read(reinterpret_cast<char*>(&meta.capacity), sizeof(size_t));

  std::vector<char> schema_data(std::istreambuf_iterator<char>(ifs), {});
  auto schema_buffer = arrow::Buffer::FromString(std::string(schema_data.begin(), schema_data.end()));
  auto reader = arrow::io::BufferReader(schema_buffer);
  meta.schema = arrow::ipc::ReadSchema(&reader, nullptr).ValueOrDie();
  return meta;
}

ArrowMeta ArrowMeta::deserialize(const std::string& input_file) {
  std::ifstream ifs(input_file, std::ios::binary);
  auto meta = deserialize(ifs);
  ifs.close();
  return meta;
}

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
      : data_manager_(std::move(data_manager)), bitmap_manager_(std::move(bitmap_manager)), meta_(meta) {}

 private:
  friend class ArrowManager;

  const MmapManager data_manager_;
  const MmapManager bitmap_manager_;
  const ArrowMeta meta_;
};

ArrowManager::ArrowManager(const std::string& location, const MmapManagerOptions& options) {
  if (!ready(location)) {
    throw std::runtime_error("ArrowManager is not ready to use");
  }

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

}  // namespace mmap_arrow
