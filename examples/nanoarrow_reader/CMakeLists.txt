add_executable(nanoarrow_reader main.cpp)

find_package(Arrow REQUIRED)

target_link_libraries(nanoarrow_reader PRIVATE
    mmap_db_lib 
    pthread
    Arrow::arrow_shared
    nanoarrow::nanoarrow
    nanoarrow::nanoarrow_ipc
)
