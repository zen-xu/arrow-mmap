cmake_minimum_required(VERSION 3.31)

project(mmap)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(quill REQUIRED)
find_package(Arrow REQUIRED)

file(GLOB_RECURSE MMAP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/mmap_arrow/*.cpp")

# 创建 mmap_arrow 库，自动包含所有源文件
add_library(mmap_arrow STATIC ${MMAP_SOURCES})
target_include_directories(mmap_arrow PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(mmap_lib INTERFACE)
target_include_directories(mmap_lib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(mmap_lib INTERFACE
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
    mmap_arrow
)

add_executable(main
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
    ${MMAP_SOURCES}  # 自动包含所有 mmap_arrow 源文件
)
target_link_libraries(main PRIVATE mmap_lib)


add_subdirectory(third_party/nanoarrow)
