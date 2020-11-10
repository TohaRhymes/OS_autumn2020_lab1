cmake_minimum_required(VERSION 3.17)
project(OS_lab1 C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -Werror -Wpedantic -Wall")

add_executable(OS_lab1 main.c)
target_link_libraries(OS_lab1 pthread)
target_link_libraries(OS_lab1 m)