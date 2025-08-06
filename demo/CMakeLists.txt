add_executable(demo main.cpp)

find_package(Arrow REQUIRED)

target_link_libraries(demo PRIVATE
    mmap_db_lib
    pthread
    Arrow::arrow_shared
)
