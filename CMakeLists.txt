cmake_minimum_required(VERSION 3.31)

project(mmap)

set(CMAKE_CXX_STANDARD 23)
find_package(quill REQUIRED)

add_executable(mmap src/main.cpp src/mmap/manager.hpp src/mmap/partition_db.hpp)

target_link_libraries(mmap PRIVATE quill::quill)
