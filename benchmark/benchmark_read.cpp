#include <memory>

#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

const size_t BATCH_SIZE = 5000;
const auto SCHEMA = arrow::schema([]() {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(8000);
  for (int i = 0; i < 8000; ++i) {
    fields.push_back(arrow::field(std::to_string(i), arrow::int32()));
  }
  return fields;
}());

static void BM_ReaderBase(benchmark::State& state) {
  auto array_length = BATCH_SIZE;
  auto capacity = BATCH_SIZE / array_length;
  auto db = mmap_db::arrow::ArrowDB("benchmark_reader_base");
  db.create(1, capacity, array_length, SCHEMA);
  auto reader = db.reader();
  auto& fields = SCHEMA->fields();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>(fields.size());

  for (auto _ : state) {
    auto field_id = 0;
    auto data_addr = reader.data_addr();

    for (const auto& field : fields) {
      const auto field_array_size = field->type()->byte_width() * array_length;
      // auto array_buffer = ::arrow::Buffer(reinterpret_cast<const uint8_t*>(data_addr), field_array_size);
      // auto array_data = ::arrow::ArrayData::Make(
      //     field->type(), array_length,
      //     {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr),
      //     field_array_size)});
      // arrays[field_id] = ::arrow::MakeArray(array_data);
    }
  }
}

class Buffer : public ::arrow::Buffer {
 public:
  Buffer(const uint8_t* data, int64_t size) {
    is_cpu_ = true;
    data_ = data;
    size_ = size;
    capacity_ = size;
    device_type_ = ::arrow::DeviceAllocationType::kCPU;
  }
};

static void BM_Reader(benchmark::State& state) {
  auto array_length = BATCH_SIZE;
  auto capacity = BATCH_SIZE / array_length;
  auto db = mmap_db::arrow::ArrowDB("benchmark_reader");
  db.create(1, capacity, array_length, SCHEMA);
  auto reader = db.reader();
  auto& fields = SCHEMA->fields();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>(fields.size());

  std::allocator<::arrow::Buffer> allocator;

  for (auto _ : state) {
    auto field_id = 0;
    auto data_addr = reader.data_addr();

    for (const auto& field : fields) {
      const auto field_array_size = field->type()->byte_width() * array_length;
      auto array_buffer = Buffer(reinterpret_cast<const uint8_t*>(data_addr), field_array_size);
      // auto array_buffer = std::allocate_shared<::arrow::Buffer>(allocator, reinterpret_cast<const
      // uint8_t*>(data_addr),
      //                                                           field_array_size);
      // auto array_buffer =
      //     std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr), field_array_size);
      // auto array_data = ::arrow::ArrayData::Make(
      //     field->type(), array_length,
      //     {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr),
      //     field_array_size)});
      // arrays[field_id] = ::arrow::MakeArray(array_data);
      data_addr += field_array_size;
      field_id++;
    }
  }
}

static void BM_ReaderMakeShared(benchmark::State& state) {
  auto array_length = BATCH_SIZE;
  auto capacity = BATCH_SIZE / array_length;
  auto db = mmap_db::arrow::ArrowDB("benchmark_reader");
  db.create(1, capacity, array_length, SCHEMA);
  auto reader = db.reader();
  auto& fields = SCHEMA->fields();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>(fields.size());

  for (auto _ : state) {
    auto field_id = 0;
    auto data_addr = reader.data_addr();

    for (const auto& field : fields) {
      const auto field_array_size = field->type()->byte_width() * array_length;
      auto array_buffer = std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data_addr), field_array_size);
      // auto array_data = ::arrow::ArrayData::Make(
      //     field->type(), array_length,
      //     {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr),
      //     field_array_size)});
      // arrays[field_id] = ::arrow::MakeArray(array_data);
      data_addr += field_array_size;
      field_id++;
    }
  }
}

static void BM_ReaderMakeShared2(benchmark::State& state) {
  auto array_length = BATCH_SIZE;
  auto capacity = BATCH_SIZE / array_length;
  auto db = mmap_db::arrow::ArrowDB("benchmark_reader");
  db.create(1, capacity, array_length, SCHEMA);
  auto reader = db.reader();
  auto& fields = SCHEMA->fields();
  auto arrays = std::vector<std::shared_ptr<::arrow::Array>>(fields.size());

  std::allocator<::arrow::Buffer> allocator;

  for (auto _ : state) {
    auto field_id = 0;
    auto data_addr = reader.data_addr();

    for (const auto& field : fields) {
      const auto field_array_size = field->type()->byte_width() * array_length;
      auto array_buffer =
          std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr), field_array_size);
      // auto array_data = ::arrow::ArrayData::Make(
      //     field->type(), array_length,
      //     {nullptr, std::make_shared<::arrow::Buffer>(reinterpret_cast<const uint8_t*>(data_addr),
      //     field_array_size)});
      // arrays[field_id] = ::arrow::MakeArray(array_data);
      data_addr += field_array_size;
      field_id++;
    }
  }
}

BENCHMARK(BM_ReaderBase)->Iterations(100);
BENCHMARK(BM_Reader)->Iterations(100);
BENCHMARK(BM_ReaderMakeShared)->Iterations(100);
BENCHMARK(BM_ReaderMakeShared2)->Iterations(100);

int main(int argc, char** argv) {
  benchmark::MaybeReenterWithoutASLR(argc, argv);
  char arg0_default[] = "benchmark";
  char* args_default = reinterpret_cast<char*>(arg0_default);
  if (!argv) {
    argc = 1;
    argv = &args_default;
  }
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  logger->set_log_level(quill::LogLevel::Critical);

  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
