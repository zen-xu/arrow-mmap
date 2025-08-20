cmake_minimum_required(VERSION 3.31)

project(mmap_arrow)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(Arrow REQUIRED)

file(GLOB_RECURSE MMAP_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/mmap_arrow/*.cpp")

add_library(${PROJECT_NAME} STATIC ${MMAP_SOURCES})
target_include_directories(${PROJECT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(${PROJECT_NAME} INTERFACE
   Arrow::arrow_shared
)

add_executable(main
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp
    ${MMAP_SOURCES}
)

add_subdirectory(third_party/nanoarrow)
