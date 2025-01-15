add_library(commonLib STATIC
        super_fast_hash.c super_fast_hash.h
        sisci_glob_defs.h
        buddy_alloc.h
        linked_list.c
        linked_list.h
)

target_include_directories(commonLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_features(commonLib PUBLIC c_std_17)