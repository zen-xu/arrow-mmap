cmake_minimum_required(VERSION 3.31)

project(mmap_arrow)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(Arrow REQUIRED)
find_package(libassert REQUIRED)

add_subdirectory(third_party/nanoarrow)

file(GLOB_RECURSE MMAP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/mmap_arrow/*.cpp")

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(${PROJECT_NAME} INTERFACE
  Arrow::arrow_shared
  libassert::assert
  nanoarrow::nanoarrow
  nanoarrow::nanoarrow_ipc
)

add_executable(main
  ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
  ${MMAP_SOURCES}
)
target_link_libraries(main PRIVATE
  ${PROJECT_NAME}
)
