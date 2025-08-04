add_executable(benchmark_mmap_db benchmark_mmap_db.cpp)

target_link_libraries(benchmark_mmap_db PRIVATE
    mmap_db_lib
    benchmark::benchmark
)
