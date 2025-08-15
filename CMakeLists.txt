cmake_minimum_required(VERSION 3.31)

project(mmap)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(quill REQUIRED)
find_package(Arrow REQUIRED)

add_library(mmap_db_lib INTERFACE)
target_include_directories(mmap_db_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_subdirectory(third_party/nanoarrow)
add_subdirectory(benchmark)
add_subdirectory(examples)
