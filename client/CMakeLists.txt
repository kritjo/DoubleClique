add_executable(client
        main.c
        main.h
        put_request_region_utils.c
        put_request_region_utils.h
        slots.h
        slots.c
        2_phase_read_get.c
        2_phase_read_get.h
        put_ack.c
        put_ack.h)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)