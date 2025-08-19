add_library(arrow_capsule SHARED lib.cpp)

# 设置 pixi 环境中的 Python 路径
set(Python3_ROOT_DIR ${CMAKE_SOURCE_DIR}/.pixi/envs/default)
set(Python3_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/.pixi/envs/default/include)
set(Python3_LIBRARIES ${CMAKE_SOURCE_DIR}/.pixi/envs/default/lib)
set(Python3_EXECUTABLE ${CMAKE_SOURCE_DIR}/.pixi/envs/default/bin/python)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

target_include_directories(arrow_capsule PRIVATE ${Python3_INCLUDE_DIRS})
target_link_libraries(arrow_capsule PRIVATE
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
    Python3::Python
)
set_target_properties(arrow_capsule PROPERTIES
    OUTPUT_NAME "arrow_capsule"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}"
)
