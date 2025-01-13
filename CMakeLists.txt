cmake_minimum_required(VERSION 3.28)
project(DoubleClique VERSION 0.0.1 LANGUAGES C)

set(CMAKE_C_STANDARD 23)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules")

find_package(SISCI REQUIRED)

add_subdirectory(common)
add_subdirectory(client)
add_subdirectory(server)
