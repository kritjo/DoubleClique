add_library(commonLib STATIC superfasthash.c superfasthash.h)

target_include_directories(commonLib
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_features(commonLib PUBLIC c_std_23)