add_executable(server main.c
        request_region_thread.c
        request_region_thread.h
        request_region_utils.c
        request_region_utils.h)

target_link_libraries(server PRIVATE commonLib)
target_link_libraries(server PRIVATE SISCI)