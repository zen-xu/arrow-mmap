add_executable(dump dump.cpp)

target_link_libraries(dump PRIVATE
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
)
set_target_properties(dump PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}"
)
