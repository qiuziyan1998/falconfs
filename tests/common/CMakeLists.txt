include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_SRC_DIR}/src/interfaces/libpq)

# ==================== FalconCMIT =================
add_executable(FalconCMIT
    ${PROJECT_SOURCE_DIR}/tests/common/test_falcon_cm.cpp
    ${common_src}
)
target_link_libraries(FalconCMIT
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

gtest_discover_tests(FalconCMIT)

# ==================== ExpiringCacheUT =================
add_executable(ExpiringCacheUT
    ${PROJECT_SOURCE_DIR}/tests/common/test_expiring_cache.cpp
    ${common_src}
)
target_link_libraries(ExpiringCacheUT
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
target_include_directories(ExpiringCacheUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
)

gtest_discover_tests(ExpiringCacheUT)
