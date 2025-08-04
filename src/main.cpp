#include <quill/Backend.h>
#include <quill/Frontend.h>
#include <quill/LogFunctions.h>
#include <quill/sinks/ConsoleSink.h>

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

int main() {
  quill::Backend::start();
  auto logger =
      quill::Frontend::create_or_get_logger("default", quill::Frontend::create_or_get_sink<quill::ConsoleSink>("sink"));
  auto db = mmap_db::PartitionDB<std::tuple<Data0, Data1>>("db");
  quill::info(logger, "init db");
  db.truncate_or_create(100);

  quill::info(logger, "init writer");
  auto writer0 = db.writer<0>();
  auto writer1 = db.writer<1>();

  quill::info(logger, "init reader");
  auto reader = db.reader();

  writer0.write({1, 2, 3, 4});
  auto data = reader.read(0);
  quill::info(logger, "reader mask buffer: {}", reader.mask_buffer_string());
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  writer1.write({5, 6, 7});
  data = reader.read(0);
  quill::info(logger, "reader mask buffer: {}", reader.mask_buffer_string());
  quill::info(logger, "data {} nullptr", data == nullptr ? "==" : "!=");

  if (nullptr != data) {
    quill::info(logger, "total size: {}", sizeof(*data));
    auto data0 = std::get<0>(*data);
    auto data1 = std::get<1>(*data);
    quill::info(logger, "data0: {}", data0.string());
    quill::info(logger, "data1: {}", data1.string());
  }

  quill::Backend::stop();
  return 0;
}
