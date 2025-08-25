cmake_minimum_required(VERSION 3.31)

project(arrow-mmap)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(Arrow REQUIRED)
find_package(libassert REQUIRED)
find_package(nanoarrow REQUIRED)

file(GLOB_RECURSE MMAP_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_mmap/*.cpp")

add_library(${PROJECT_NAME} ${MMAP_SOURCES})
target_include_directories(${PROJECT_NAME}
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  ${PROJECT_NAME} PUBLIC Arrow::arrow_shared libassert::assert
                         nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)

add_subdirectory(benchmark)
add_subdirectory(example)
