add_executable(server main.c
        put_request_region_thread.c
        put_request_region_thread.h
        put_request_region_utils.c
        put_request_region_utils.h)

target_link_libraries(server PRIVATE commonLib)
target_link_libraries(server PRIVATE SISCI)