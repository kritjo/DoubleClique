add_executable(client
        main.c
        main.h
        put_request_region_utils.c
        put_request_region_utils.h
        slots.h
        slots.c
        contingency_get_fetch.c
        contingency_get_fetch.h
        get_protocol.h)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)