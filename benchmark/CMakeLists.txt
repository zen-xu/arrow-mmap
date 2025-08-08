add_executable(benchmark_mmap benchmark.cpp)

find_package(Arrow REQUIRED)

target_link_libraries(benchmark_mmap PRIVATE
    mmap_db_lib
    Arrow::arrow_shared
    benchmark::benchmark
    pthread
)
