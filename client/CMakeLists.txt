add_executable(client
        main.c
        main.h
        2_phase_read_get.c
        2_phase_read_get.h
        put.h
        put.c
        request_region_connection.c
        request_region_connection.h)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)