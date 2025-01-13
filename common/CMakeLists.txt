add_library(commonLib STATIC super_fast_hash.c super_fast_hash.h)

target_include_directories(commonLib
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(commonLib PUBLIC c_std_23)