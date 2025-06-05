project(DBStress)

find_package(gflags REQUIRED)

add_executable(concurrent_test concurrent_test.cpp)
target_link_libraries(concurrent_test PRIVATE gflags test_util)

add_executable(test_client test_client.cpp)
target_link_libraries(test_client PRIVATE gflags test_util)
