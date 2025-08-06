
#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/dyn_partition_db.hpp"
#include "mmap/partition_db.hpp"

struct Data0 {
  int a, b, c, d;
};

struct Data1 {
  int a, b;
  double c;
};

static void BM_WritePerformance(benchmark::State& state) {
  auto db = mmap_db::PartitionDB<std::tuple<Data0, Data1>>("benchmark_db");
  int capacity = 10000000;
  db.truncate_or_create(capacity, true);
  auto writer = db.writer<0>();

  Data0 data0{1, 1, 1, 1};
  for (auto _ : state) {
    for (int i = 0; i < capacity; i++) {
      writer.write(data0, i);
    }
  }
}

static void BM_DynWritePerformance(benchmark::State& state) {
  auto db = mmap_db::DynPartitionDB<mmap_db::DynPartitionOrder::C>("benchmark_dyn_db");
  int capacity = 10;
  db.create(capacity, {sizeof(Data0)});
  auto writer = db.writer(0);

  Data0 data0{1, 1, 1, 1};
  for (auto _ : state) {
    for (int i = 0; i < capacity; i++) {
      writer.write(&data0, i);
    }
  }
}

static void BM_DynWritePerformance2(benchmark::State& state) {
  auto db = mmap_db::DynPartitionDB<mmap_db::DynPartitionOrder::C>("benchmark_dyn_db");
  int capacity = 10;
  db.create(capacity, {sizeof(Data0)});
  auto writer = db.writer(0);

  Data0 data0{1, 1, 1, 1};
  auto addr = writer.addr();
  for (auto _ : state) {
    for (int i = 0; i < capacity; i++) {
      std::memcpy(addr + i * sizeof(Data0), &data0, sizeof(Data0));
    }
  }
}

// 注册基准测试
// BENCHMARK(BM_WritePerformance)->Iterations(100);
BENCHMARK(BM_DynWritePerformance)->Iterations(100000000);
BENCHMARK(BM_DynWritePerformance2)->Iterations(100000000);

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

  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
