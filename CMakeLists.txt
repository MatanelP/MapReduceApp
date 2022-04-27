cmake_minimum_required(VERSION 3.21)
project(Ex3)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
add_executable(Ex3 MapReduceClient.h MapReduceFramework.h SampleClient.cpp MapReduceFramework.cpp)
