add_library(commonLib STATIC
        super_fast_hash.c
        super_fast_hash.h
        sisci_glob_defs.h
        buddy_alloc.h
        request_region.h
        index_data_protocol.h
        get_node_id.c
        get_node_id.h
        power_of_two.h
        power_of_two.c
        index_data_protocol.c
        segment_utils.c
        segment_utils.h
        dma_utils.c
        dma_utils.h
        sequence.c
        sequence.h
        profiling.h
)

target_include_directories(commonLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_features(commonLib PUBLIC c_std_17)

target_link_libraries(commonLib PRIVATE SISCI)