#include "arrow_mmap/arrow_reader.hpp"

#include <libassert/assert.hpp>

namespace arrow_mmap {

inline ArrowType as_nanoarrow_type(arrow::Type::type type) {
  switch (type) {
    case arrow::Type::NA:
      return NANOARROW_TYPE_NA;
    case arrow::Type::BOOL:
      return NANOARROW_TYPE_BOOL;
    case arrow::Type::UINT8:
      return NANOARROW_TYPE_UINT8;
    case arrow::Type::INT8:
      return NANOARROW_TYPE_INT8;
    case arrow::Type::UINT16:
      return NANOARROW_TYPE_UINT16;
    case arrow::Type::INT16:
      return NANOARROW_TYPE_INT16;
    case arrow::Type::UINT32:
      return NANOARROW_TYPE_UINT32;
    case arrow::Type::INT32:
      return NANOARROW_TYPE_INT32;
    case arrow::Type::UINT64:
      return NANOARROW_TYPE_UINT64;
    case arrow::Type::INT64:
      return NANOARROW_TYPE_INT64;
    case arrow::Type::HALF_FLOAT:
      return NANOARROW_TYPE_HALF_FLOAT;
    case arrow::Type::FLOAT:
      return NANOARROW_TYPE_FLOAT;
    case arrow::Type::DOUBLE:
      return NANOARROW_TYPE_DOUBLE;
    case arrow::Type::STRING:
      return NANOARROW_TYPE_STRING;
    case arrow::Type::BINARY:
      return NANOARROW_TYPE_BINARY;
    case arrow::Type::FIXED_SIZE_BINARY:
      return NANOARROW_TYPE_FIXED_SIZE_BINARY;
    case arrow::Type::DATE32:
      return NANOARROW_TYPE_DATE32;
    case arrow::Type::DATE64:
      return NANOARROW_TYPE_DATE64;
    case arrow::Type::TIMESTAMP:
      return NANOARROW_TYPE_TIMESTAMP;
    case arrow::Type::TIME32:
      return NANOARROW_TYPE_TIME32;
    case arrow::Type::TIME64:
      return NANOARROW_TYPE_TIME64;
    case arrow::Type::INTERVAL_MONTHS:
      return NANOARROW_TYPE_INTERVAL_MONTHS;
    case arrow::Type::INTERVAL_DAY_TIME:
      return NANOARROW_TYPE_INTERVAL_DAY_TIME;
    case arrow::Type::DECIMAL128:
      return NANOARROW_TYPE_DECIMAL128;
    case arrow::Type::DECIMAL256:
      return NANOARROW_TYPE_DECIMAL256;
    case arrow::Type::LIST:
      return NANOARROW_TYPE_LIST;
    case arrow::Type::STRUCT:
      return NANOARROW_TYPE_STRUCT;
    case arrow::Type::SPARSE_UNION:
      return NANOARROW_TYPE_SPARSE_UNION;
    case arrow::Type::DENSE_UNION:
      return NANOARROW_TYPE_DENSE_UNION;
    case arrow::Type::DICTIONARY:
      return NANOARROW_TYPE_DICTIONARY;
    case arrow::Type::MAP:
      return NANOARROW_TYPE_MAP;
    case arrow::Type::EXTENSION:
      return NANOARROW_TYPE_EXTENSION;
    case arrow::Type::FIXED_SIZE_LIST:
      return NANOARROW_TYPE_FIXED_SIZE_LIST;
    case arrow::Type::DURATION:
      return NANOARROW_TYPE_DURATION;
    case arrow::Type::LARGE_STRING:
      return NANOARROW_TYPE_LARGE_STRING;
    case arrow::Type::LARGE_BINARY:
      return NANOARROW_TYPE_LARGE_BINARY;
    case arrow::Type::LARGE_LIST:
      return NANOARROW_TYPE_LARGE_LIST;
    case arrow::Type::INTERVAL_MONTH_DAY_NANO:
      return NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
    case arrow::Type::RUN_END_ENCODED:
      return NANOARROW_TYPE_RUN_END_ENCODED;
    case arrow::Type::BINARY_VIEW:
      return NANOARROW_TYPE_BINARY_VIEW;
    case arrow::Type::STRING_VIEW:
      return NANOARROW_TYPE_STRING_VIEW;
    default:
      return NANOARROW_TYPE_UNINITIALIZED;
  }
}

ArrowReader::ArrowReader(const ArrowMeta meta, const IMmapReader* data_reader, const IMmapReader* bitflag_reader)
    : meta_(meta),
      data_reader_(data_reader),
      bitflag_reader_(bitflag_reader),
      batch_size_(std::accumulate(meta_.schema->fields().begin(), meta_.schema->fields().end(), 0,
                                  [](size_t acc, const auto& field) { return acc + field->type()->byte_width(); })),
      col_sizes_([&]() {
        std::vector<size_t> col_sizes;
        for (const auto& field : meta.schema->fields()) {
          col_sizes.push_back(field->type()->byte_width());
        }
        return col_sizes;
      }()),
      col_types_([&]() {
        std::vector<ArrowType> col_types;
        for (const auto& field : meta.schema->fields()) {
          col_types.push_back(as_nanoarrow_type(field->type()->id()));
        }
        return col_types;
      }()),
      schema_([&]() {
        nanoarrow::UniqueSchema schema;

        NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRUCT));
        NANOARROW_THROW_NOT_OK(ArrowSchemaAllocateChildren(schema.get(), col_types_.size()));

        auto& fields = meta.schema->fields();
        for (size_t i = 0; i < col_types_.size(); i++) {
          auto& field = fields[i];
          NANOARROW_THROW_NOT_OK(ArrowSchemaInitFromType(schema->children[i], col_types_[i]));
          NANOARROW_THROW_NOT_OK(ArrowSchemaSetName(schema->children[i], field->name().c_str()));
        }
        return schema;
      }()),
      struct_array_([&]() {
        nanoarrow::UniqueArray struct_array;
        NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(struct_array.get(), NANOARROW_TYPE_STRUCT));
        NANOARROW_THROW_NOT_OK(ArrowArrayAllocateChildren(struct_array.get(), col_types_.size()));

        for (size_t i = 0; i < col_types_.size(); i++) {
          NANOARROW_THROW_NOT_OK(ArrowArrayInitFromType(struct_array->children[i], col_types_[i]));
        }
        return struct_array;
      }()) {}

bool ArrowReader::read(nanoarrow::UniqueArrayStream& stream) {
  auto ret = read(stream, index_);
  if (ret) index_++;
  return ret;
}

bool ArrowReader::read(nanoarrow::UniqueArrayStream& stream, const size_t index) {
  ASSERT(index < meta_.capacity, "index out of range, index: {}, capacity: {}", index, meta_.capacity);

  auto bitflag_addr = bitflag_reader_->mmap_addr() + index * meta_.writer_count;
  if (!std::all_of(bitflag_addr, bitflag_addr + meta_.writer_count,
                   [](const std::byte& b) { return b == std::byte(0xff); })) {
    return false;
  }

  auto data_addr = data_reader_->mmap_addr() + index * batch_size_;
  for (size_t i = 0; i < col_sizes_.size(); i++) {
    struct_array_->children[i]->buffers[1] = reinterpret_cast<const void*>(data_addr);
    struct_array_->children[i]->length = meta_.array_length;
    // do not release the buffer
    struct_array_->children[i]->release = nullptr;
    data_addr += col_sizes_[i] * meta_.array_length;
  }
  struct_array_->length = meta_.array_length;

  NANOARROW_THROW_NOT_OK(ArrowBasicArrayStreamInit(stream.get(), schema_.get(), 1));
  ArrowBasicArrayStreamSetArray(stream.get(), 0, struct_array_.get());

  return true;
}
}  // namespace arrow_mmap
