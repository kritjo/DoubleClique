add_executable(server main.c
        put_request_region.c
        put_request_region.h)

target_link_libraries(server PRIVATE commonLib)
target_link_libraries(server PRIVATE SISCI)