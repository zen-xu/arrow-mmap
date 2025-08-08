
#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"
#include "mmap/dyn_partition_db.hpp"

struct Data0 {
  int a, b, c, d;
};

struct Data1 {
  int a, b;
  double c;
};

struct Row {
  int32_t id;
  char c;
} __attribute__((packed));

template <typename T>
void memcpy(std::byte* dest, const void* src) {
  std::memcpy(dest + sizeof(T), src, sizeof(T));
}

static void BM_DynWritePerformance(benchmark::State& state) {
  auto db = mmap_db::DynPartitionDB<mmap_db::DynPartitionOrder::C>("benchmark_dyn_db");
  int capacity = 1;
  db.create(capacity, {sizeof(Data0)});
  auto writer = db.writer(0);

  Data0 data0{1, 1, 1, 1};
  auto addr = writer.addr();
  for (auto _ : state) {
    memcpy<Data0>(addr, &data0);
    // std::memcpy(addr + sizeof(Data0), &data0, sizeof(Data0));
  }
}

static void BM_ArrowWritePerformance(benchmark::State& state) {
  auto db = mmap_db::arrow::ArrowDB("benchmark_arrow_db");
  auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::fixed_size_binary(1))});
  db.create(1, state.max_iterations, schema);

  auto writer = db.writer(0);
  auto row = Row{1, 'a'};
  for (auto _ : state) {
    writer.write(row);
  }
}

// 注册基准测试
BENCHMARK(BM_ArrowWritePerformance)->Iterations(100000);
BENCHMARK(BM_DynWritePerformance)->Iterations(100000);

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
