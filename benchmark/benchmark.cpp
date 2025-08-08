
#include <thread>

#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"
#include "mmap/dyn_partition_db.hpp"

struct Row {
  int32_t id;
  char c;
} __attribute__((packed));

static void BM_MemcpyPerformance(benchmark::State& state) {
  auto db = mmap_db::DynPartitionDB<mmap_db::DynPartitionOrder::C>("benchmark_dyn_db");
  int batch_size = 1000000;
  db.create(batch_size, {sizeof(Row)});
  auto writer = db.writer(0);

  Row row{1, 'a'};
  auto addr = writer.addr();
  for (auto _ : state) {
    for (auto i = 0; i < batch_size; ++i) {
      std::memcpy(addr + i * sizeof(Row), &row, sizeof(Row));
    }
  }
}

void write_thread(mmap_db::arrow::ArrowDB& db, int batch_size, int writer_id) {
  auto writer = db.writer(writer_id);
  auto row = Row{1, 'a'};
  for (auto i = 0; i < batch_size; ++i) {
    writer.write(row, i);
  }
}

static void BM_ArrowWritePerformance(benchmark::State& state) {
  auto db = mmap_db::arrow::ArrowDB("benchmark_arrow_db");
  auto schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::fixed_size_binary(1))});
  auto batch_size = 1000000;
  db.create(2, batch_size, schema);

  for (auto _ : state) {
    std::thread t0(write_thread, std::ref(db), batch_size, 0);
    std::thread t1(write_thread, std::ref(db), batch_size, 1);
    t0.join();
    t1.join();
  }
}

// 注册基准测试
BENCHMARK(BM_ArrowWritePerformance)->Iterations(100);
BENCHMARK(BM_MemcpyPerformance)->Iterations(100);

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
