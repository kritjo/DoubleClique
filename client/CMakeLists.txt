add_executable(client
        main.c
        main.h
        put.c
        put.h
        slots.h
        slots.c)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)