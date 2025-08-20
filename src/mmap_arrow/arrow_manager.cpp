#include "arrow_manager.hpp"

#include <fstream>
#include <vector>

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace mmap_arrow {

void ArrowMeta::serialize(std::ofstream& ofs) const {
  auto schema_buffer = arrow::ipc::SerializeSchema(*schema).ValueOrDie();
  ofs.write(reinterpret_cast<const char*>(&writer_count), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(&array_length), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(&capacity), sizeof(size_t));
  ofs.write(reinterpret_cast<const char*>(schema_buffer->data()), schema_buffer->size());
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

class ArrowManager::Impl {
 public:
  Impl(const std::string& location, const ArrowMeta meta, const MmapManagerOptions& options)
      : data_file_(location + "/data.mmap"), bitmap_file_(location + "/bitmap.mmap"), meta_(meta), options_(options) {}

 private:
  const std::string data_file_;
  const std::string bitmap_file_;
  const ArrowMeta meta_;
  const MmapManagerOptions options_;
};

ArrowManager::ArrowManager(const std::string& location, const MmapManagerOptions& options) {}

ArrowManager::~ArrowManager() { delete impl_; }

ArrowManager ArrowManager::create(const std::string& location, const size_t writer_count, const size_t array_length,
                                  const size_t capacity, const std::shared_ptr<arrow::Schema> schema,
                                  const MmapManagerCreateOptions& options) {}

}  // namespace mmap_arrow
