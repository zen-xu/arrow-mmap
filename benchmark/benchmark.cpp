#include <benchmark/benchmark.h>

#include "arrow_mmap/arrow_manager.hpp"

const size_t BATCH_SIZE = 5000;
const auto SCHEMA = arrow::schema([]() {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(8000);
  for (int i = 0; i < 8000; ++i) {
    fields.push_back(arrow::field(std::to_string(i), arrow::int32()));
  }
  return fields;
}());

static void BM_Reader(benchmark::State& state) {
  auto array_length = 100;
  auto capacity = BATCH_SIZE / array_length;
  auto manager = mmap_arrow::ArrowManager::create("benchmark_reader", 1, array_length, capacity, SCHEMA,
                                                  {.fill_with = std::byte(0xff)});
  nanoarrow::UniqueArrayStream stream;
  auto reader = manager.reader();
  for (auto _ : state) {
    for (size_t i = 0; i < capacity; i++) {
      reader->read(stream, i);
    }
  }
}

BENCHMARK(BM_Reader)->Iterations(100);
BENCHMARK_MAIN();
