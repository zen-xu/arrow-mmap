add_executable(benchmark_mmap benchmark.cpp)
add_executable(benchmark_read benchmark_read.cpp)

find_package(benchmark REQUIRED)
find_package(Arrow REQUIRED)

target_link_libraries(benchmark_mmap PRIVATE
    mmap_db_lib
    Arrow::arrow_shared
    benchmark::benchmark
    pthread
)

target_link_libraries(benchmark_read PRIVATE
    mmap_db_lib
    Arrow::arrow_shared
    benchmark::benchmark
    pthread
)
