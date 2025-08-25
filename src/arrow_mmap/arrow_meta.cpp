#include "arrow_meta.hpp"

#include <fstream>

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
}  // namespace mmap_arrow
