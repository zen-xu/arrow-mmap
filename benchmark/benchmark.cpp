#include <benchmark/benchmark.h>
#include <sys/mman.h>

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

static void BM_ReaderNormal(benchmark::State& state) {
  auto array_length = 100;
  auto capacity = BATCH_SIZE / array_length;
  auto manager = arrow_mmap::ArrowManager::create("benchmark_reader_normal", 1, array_length, capacity, SCHEMA,
                                                  {.madvise = MADV_NORMAL, .fill_with = std::byte(0xff)});
  nanoarrow::UniqueArrayStream stream;
  auto reader = manager.reader();
  for (auto _ : state) {
    for (size_t i = 0; i < capacity; i++) {
      reader->read(stream, i);
    }
  }
}

static void BM_ReaderWillNeed(benchmark::State& state) {
  auto array_length = 100;
  auto capacity = BATCH_SIZE / array_length;
  auto manager = arrow_mmap::ArrowManager::create("benchmark_reader", 1, array_length, capacity, SCHEMA,
                                                  {.madvise = MADV_WILLNEED, .fill_with = std::byte(0xff)});
  nanoarrow::UniqueArrayStream stream;
  auto reader = manager.reader();
  for (auto _ : state) {
    for (size_t i = 0; i < capacity; i++) {
      reader->read(stream, i);
    }
  }
}

static void BM_ReaderWillNeedPopulate(benchmark::State& state) {
  auto array_length = 100;
  auto capacity = BATCH_SIZE / array_length;
  auto manager = arrow_mmap::ArrowManager::create(
      "benchmark_reader", 1, array_length, capacity, SCHEMA,
      {.reader_flags = MAP_POPULATE, .madvise = MADV_WILLNEED, .fill_with = std::byte(0xff)});
  nanoarrow::UniqueArrayStream stream;
  auto reader = manager.reader();
  for (auto _ : state) {
    for (size_t i = 0; i < capacity; i++) {
      reader->read(stream, i);
    }
  }
}

BENCHMARK(BM_ReaderNormal)->Iterations(100);
BENCHMARK(BM_ReaderWillNeed)->Iterations(100);
BENCHMARK(BM_ReaderWillNeedPopulate)->Iterations(100);
BENCHMARK_MAIN();
