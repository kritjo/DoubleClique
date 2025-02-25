add_executable(client
        main.c
        main.h
        2_phase_read_get.c
        2_phase_read_get.h
        put.h
        put.c)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)