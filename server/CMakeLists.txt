add_executable(server main.c)

target_link_libraries(server PRIVATE commonLib)
target_link_libraries(server PRIVATE SISCI)