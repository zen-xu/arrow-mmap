add_executable(example main.cpp)
target_link_libraries(example PRIVATE ${PROJECT_NAME})
set_target_properties(example PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                         "${CMAKE_CURRENT_SOURCE_DIR}")
