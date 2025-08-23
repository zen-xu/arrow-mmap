add_executable(benchmark benchmark.cpp)

find_package(benchmark REQUIRED)
find_package(Arrow REQUIRED)

target_link_libraries(benchmark PRIVATE
  ${PROJECT_NAME}
  benchmark::benchmark
)