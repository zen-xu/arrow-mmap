
#include <thread>

#include <benchmark/benchmark.h>
#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/arrow_db.hpp"

const auto SCHEMA = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("age", arrow::int32())});
const size_t BATCH_SIZE = 1000000;

#define RETURN_IF_STATUS_NOT_OK(s) \
  if (!(s).ok()) {                 \
    return nullptr;                \
  }

const std::shared_ptr<arrow::RecordBatch> create_batch(std::vector<int32_t> ids, std::vector<int32_t> ages) {
  std::shared_ptr<arrow::Array> id_array;
  std::shared_ptr<arrow::Array> age_array;
  arrow::Int32Builder id_builder, age_builder;
  RETURN_IF_STATUS_NOT_OK(id_builder.AppendValues(ids));
  RETURN_IF_STATUS_NOT_OK(age_builder.AppendValues(ages));
  RETURN_IF_STATUS_NOT_OK(id_builder.Finish(&id_array));
  RETURN_IF_STATUS_NOT_OK(age_builder.Finish(&age_array));
  return arrow::RecordBatch::Make(SCHEMA, ids.size(), {id_array, age_array});
}
const std::shared_ptr<arrow::RecordBatch> batch1 = create_batch({1, 2, 3, 4, 5}, {21, 22, 23, 24, 25});
const std::shared_ptr<arrow::RecordBatch> batch2 = create_batch({6, 7, 8, 9, 10}, {26, 27, 28, 29, 30});

void write1_thread(mmap_db::arrow::ArrowDB& db) {
  auto writer = db.writer(0);
  for (auto i = 0; i < BATCH_SIZE; ++i) {
    writer.write(batch1, i);
  }
}

void write2_thread(mmap_db::arrow::ArrowDB& db) {
  auto writer = db.writer(1);
  for (auto i = 0; i < BATCH_SIZE; ++i) {
    writer.write(batch2, i);
  }
}

static void BM_ArrowDBPerformance(benchmark::State& state) {
  auto db = mmap_db::arrow::ArrowDB("benchmark_arrow_db");
  db.create(1, BATCH_SIZE, 10, SCHEMA);
  auto writer1 = db.writer(0);
  auto writer2 = db.writer(0);

  for (auto _ : state) {
    for (auto i = 0; i < BATCH_SIZE; ++i) {
      writer1.write(batch1, i);
      writer2.write(batch2, i);
    }
  }
}

static void BM_ArrowDBThreadPerformance(benchmark::State& state) {
  auto db = mmap_db::arrow::ArrowDB("benchmark_thread_arrow_db");
  db.create(2, BATCH_SIZE, 10, SCHEMA);

  for (auto _ : state) {
    std::thread t0(write1_thread, std::ref(db));
    std::thread t1(write2_thread, std::ref(db));
    t0.join();
    t1.join();
  }
}

// // 注册基准测试
BENCHMARK(BM_ArrowDBPerformance)->Iterations(10);
// BENCHMARK(BM_ArrowDBThreadPerformance)->Iterations(10);
// BENCHMARK(BM_ArrowDBThreadPerformance)->Iterations(100);

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
