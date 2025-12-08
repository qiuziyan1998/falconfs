include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_SRC_DIR}/src/interfaces/libpq)

# ==================== UtilsUT =================
add_executable(UtilsUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_db/test_utils.cpp
    ${PROJECT_SOURCE_DIR}/falcon/utils/utils_standalone.c
)

target_link_libraries(UtilsUT
    gtest
    pq
    ${DYNAMIC_LIB}
)

target_include_directories(UtilsUT PUBLIC
    ${PROJECT_SOURCE_DIR}/falcon_store/src/include
    ${PROJECT_SOURCE_DIR}/falcon_client/src/include
    ${PROJECT_SOURCE_DIR}/common/src/include
    ${PROJECT_SOURCE_DIR}/falcon/include
    ${POSTGRES_SRC_DIR}/src/include
    ${POSTGRES_SRC_DIR}/src/interfaces/libpq
)

gtest_discover_tests(UtilsUT)