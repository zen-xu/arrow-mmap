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
  auto array_length = 10;
  auto capacity = BATCH_SIZE / array_length;
  auto db = mmap_db::arrow::ArrowDB("benchmark_reader_base");
  db.create(1, capacity, array_length, SCHEMA);

  nanoarrow::UniqueArrayStream array_stream;
  auto reader = db.reader();
  for (auto _ : state) {
    for (size_t i = 0; i < capacity; i++) {
      reader.read(array_stream, i);
    }
  }
}

BENCHMARK(BM_ReaderBase)->Iterations(100);

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

  logger->set_log_level(quill::LogLevel::Error);

  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
