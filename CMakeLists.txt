cmake_minimum_required(VERSION 3.31)

project(mmap)

set(CMAKE_CXX_STANDARD 23)
find_package(quill REQUIRED)
find_package(benchmark REQUIRED)

# 创建库目标
add_library(mmap_db_lib INTERFACE)
target_include_directories(mmap_db_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# 主程序
add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE
    quill::quill
    mmap_db_lib)

add_subdirectory(benchmark)
