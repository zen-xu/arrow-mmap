cmake_minimum_required(VERSION 3.31)

project(arrow-mmap)

set(CMAKE_CXX_STANDARD 23)
set(NANOARROW_IPC ON)

find_package(Arrow REQUIRED)
find_package(libassert REQUIRED)
find_package(Git REQUIRED)

execute_process(
  COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  RESULT_VARIABLE GIT_SUBMODULE_RESULT)

if(NOT GIT_SUBMODULE_RESULT EQUAL 0)
  message(
    FATAL_ERROR
      "git submodule update --init --recursive failed with ${GIT_SUBMODULE_RESULT}, please checkout submodules"
  )
endif()

add_subdirectory(third_party/nanoarrow)

file(GLOB_RECURSE MMAP_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/src/mmap_arrow/*.cpp")

add_library(${PROJECT_NAME} ${MMAP_SOURCES})
target_include_directories(${PROJECT_NAME}
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  ${PROJECT_NAME} PUBLIC Arrow::arrow_shared libassert::assert
                         nanoarrow::nanoarrow nanoarrow::nanoarrow_ipc)

add_executable(main ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp ${MMAP_SOURCES})
target_link_libraries(main PRIVATE ${PROJECT_NAME})

add_subdirectory(benchmark)
