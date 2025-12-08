include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_SRC_DIR}/src/interfaces/libpq)

# ==================== RouterUT =================
add_executable(RouterUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_client/test_router.cpp
    ${common_src}
)
target_link_libraries(RouterUT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

target_include_directories(RouterUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)

gtest_discover_tests(RouterUT)

# ==================== ConnectionUT =================
add_executable(ConnectionUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_client/test_connection.cpp
    ${common_src}
)
target_link_libraries(ConnectionUT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

target_include_directories(ConnectionUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)

gtest_discover_tests(ConnectionUT)