add_executable(client
        main.c
        main.h
        put_request_region_utils.c
        put_request_region_utils.h
        slots.h
        slots.c)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)