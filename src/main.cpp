#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

#include "mmap/dyn_partition_db.hpp"
#include "mmap/partition_db.hpp"

struct Data0 {
  int a, b, c, d;

  std::string string() { return std::format("a: {}, b: {}, c: {}, d: {}", a, b, c, d); }
};

struct Data1 {
  int a, b;
  double c;

  std::string string() { return std::format("a: {}, b: {}, c: {}", a, b, c); }
};

int main2() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));
  auto db = mmap_db::PartitionDB<std::tuple<Data0, Data1>>("db");
  quill::info(logger, "init db");
  db.truncate_or_create(100, true);

  quill::info(logger, "init writer");
  auto writer0 = db.writer<0>();
  auto writer1 = db.writer<1>();

  quill::info(logger, "init reader");
  auto reader = db.reader();

  writer0.write({1, 2, 3, 4});
  auto data = reader.read(0);
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  writer1.write({5, 6, 7});
  data = reader.read(0);
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  if (nullptr != data) {
    auto& data0 = std::get<0>(*data);
    auto& data1 = std::get<1>(*data);
    quill::info(logger, "idx 0 >> data0: {}, data1: {}", data0.string(), data1.string());

    quill::info(logger, "idx 0 >> rewrite data0");
    writer0.write({123, 2, 3, 4}, 0);
    quill::info(logger, "idx 0 >> data0: {}, data1: {}", data0.string(), data1.string());
  }

  writer0.write({10, 11, 12, 13}, 1);
  writer1.write({14, 15, 16}, 1);
  data = reader.read();  // skip index 0
  data = reader.read();
  if (nullptr != data) {
    auto& data0 = std::get<0>(*data);
    auto& data1 = std::get<1>(*data);
    quill::info(logger, "idx 1 >> data0: {}, data1: {}", data0.string(), data1.string());
  }

  quill::Backend::stop();
  return 0;
}

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));
  quill::info(logger, "init db");
  auto db = mmap_db::DynPartitionDB("db");
  db.create(100, {sizeof(Data0), sizeof(Data1)});
  quill::info(logger, "init writer");

  auto writer0 = db.writer(0);
  auto writer1 = db.writer(1);

  quill::info(logger, "init reader");
  auto reader = db.reader();

  Data0 data0{1, 2, 3, 4};
  if (!writer0.write(&data0)) {
    return 1;
  }
  auto data = reader.read(0);
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  Data1 data1{5, 6, 7};
  if (!writer1.write(&data1)) {
    return 1;
  }
  data = reader.read(0);
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  if (nullptr != data) {
    auto data0 = reinterpret_cast<Data0*>(data);
    auto data1 = reinterpret_cast<Data1*>(data + sizeof(Data0));
    quill::info(logger, "data0: {}, data1: {}", data0->string(), data1->string());
  }

  quill::Backend::stop();
  return 0;
}
