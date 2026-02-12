cmake_minimum_required(VERSION 3.28)
project(DoubleClique VERSION 0.0.1 LANGUAGES C)

set(CMAKE_C_STANDARD 17)

set(CMAKE_C_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")
set(CMAKE_BUILD_TYPE Release)

add_compile_options(
    -Wall
    -Wextra
    -pedantic
    -Wshadow
    -Wformat=2
    -Wfloat-equal
    -Wconversion
    -Wpointer-arith
    -Wcast-qual
    -Wstrict-overflow=5
    -Wswitch-default
    -Wswitch-enum
    -Wunreachable-code
    -Wunused
    -Wredundant-decls
    -Wwrite-strings
    -Wlogical-op
)
add_compile_definitions(_REENTRANT)
add_compile_definitions(_POSIX_C_SOURCE=200112L)

set(WARNING_AS_ERROR 0)

if (WARNING_AS_ERROR EQUAL 1)
    add_compile_options(-Werror)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

find_package(SISCI REQUIRED)

add_subdirectory(common)
add_subdirectory(client)
add_subdirectory(server)
