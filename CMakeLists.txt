cmake_minimum_required(VERSION 3.31)

project(mmap)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)
find_package(quill REQUIRED)
find_package(benchmark REQUIRED)
find_package(Arrow REQUIRED)

add_subdirectory(third_party/nanoarrow)

# 创建库目标
add_library(mmap_db_lib INTERFACE)
target_include_directories(mmap_db_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# 主程序
add_executable(main src/main.cpp)
target_link_libraries(main PRIVATE
    quill::quill
    pthread
    Arrow::arrow_shared
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
    mmap_db_lib)

add_subdirectory(benchmark)
add_subdirectory(examples/ipc_read)
