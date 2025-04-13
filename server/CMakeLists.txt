add_executable(server main.c
        request_region_thread.c
        request_region_thread.h
        request_region_utils.c
        request_region_utils.h
        garbage_collection.c
        garbage_collection.h
        garbage_collection_queue.c
        garbage_collection_queue.h)

target_link_libraries(server PRIVATE commonLib)
target_link_libraries(server PRIVATE SISCI)
target_link_libraries(server PRIVATE xxhash)