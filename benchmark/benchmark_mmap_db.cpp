
#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/partition_db.hpp"

struct Data0 {
  int a, b, c, d;
};

struct Data1 {
  int a, b;
  double c;
};

// 基准测试：写入性能
static void BM_WritePerformance(benchmark::State& state) {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));

  auto db = mmap_db::PartitionDB<std::tuple<Data0, Data1>>("benchmark_db");
  int capacity = 10000000;
  db.truncate_or_create(capacity, true);
  auto writer = db.writer<0>();

  for (auto _ : state) {
    for (int i = 0; i < capacity; i++) {
      writer.write({1, 1, 1, 1}, i);
    }
  }
}

// 注册基准测试
BENCHMARK(BM_WritePerformance)->Iterations(100);

BENCHMARK_MAIN();
