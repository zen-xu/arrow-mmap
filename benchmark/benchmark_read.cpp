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

mmap_db::arrow::ArrowDB init_db(const std::string& name, int array_length, int capacity) {
  auto db = mmap_db::arrow::ArrowDB(name);
  db.create(1, capacity, array_length, SCHEMA);
  return db;
}

static void BM_Read10(benchmark::State& state) {
  auto array_length = BATCH_SIZE;
  auto capacity = BATCH_SIZE / array_length;
  auto db = init_db("benchmark_arrow_db", array_length, capacity);
  auto reader = db.reader();
  for (auto _ : state) {
    for (auto i = 0; i < capacity; i++) {
      reader.read(i);
    }
  }
}

BENCHMARK(BM_Read10)->Iterations(10);

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
