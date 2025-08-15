#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.hpp>

#include <nanoarrow/nanoarrow_ipc.h>

bool WriteSchemaToFile(const std::string& filepath, nanoarrow::UniqueSchema& schema) {
  // 打开文件
  FILE* file = fopen(filepath.c_str(), "wb");
  if (!file) {
    return false;
  }

  // 创建输出流
  nanoarrow::ipc::UniqueOutputStream output_stream;
  struct ArrowError error;
  if (ArrowIpcOutputStreamInitFile(output_stream.get(), file, true) != NANOARROW_OK) {
    fclose(file);
    return false;
  }

  // 创建 writer
  nanoarrow::ipc::UniqueWriter writer;
  if (ArrowIpcWriterInit(writer.get(), output_stream.get()) != NANOARROW_OK) {
    return false;
  }

  if (ArrowIpcWriterWriteSchema(writer.get(), schema.get(), &error) != NANOARROW_OK) {
    return false;
  }

  return true;
}

int main() {
  // 使用更简洁的方式创建 schema
  nanoarrow::UniqueSchema schema;

  // 直接创建 struct schema，一次性设置所有属性
  NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRUCT));

  // 分配并设置子列
  NANOARROW_RETURN_NOT_OK(ArrowSchemaAllocateChildren(schema.get(), 2));

  // 设置列1：long类型
  NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema->children[0], NANOARROW_TYPE_INT64));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema->children[0], "col1"));

  // 设置列2：unsigned int类型
  NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema->children[1], NANOARROW_TYPE_UINT32));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema->children[1], "col2"));

  // 写入文件
  bool success = WriteSchemaToFile("schema.arrow", schema);

  return success ? 0 : 1;
}
