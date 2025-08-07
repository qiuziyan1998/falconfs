cmake_minimum_required(VERSION 3.15)

project(MyProject)

add_definitions(-D_FILE_OFFSET_BITS=64)
set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
set(CMAKE_CXX_STANDARD 17)

add_executable(test_posix main.cc workloads.cc posix.cc)

target_link_libraries(test_posix 
    PUBLIC pthread
    PUBLIC fmt
)

link_directories(
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
)
include_directories(
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)

add_executable(test_falcon main.cc workloads.cc falconfs.cc)

target_link_libraries(test_falcon
    FalconClient
    FalconStore
    pthread
    pq
    fuse
    zookeeper_mt
    glog
    jsoncpp
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    dl
)
