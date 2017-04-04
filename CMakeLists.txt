set(CMAKE_BUILD_TYPE Release CACHE STRING "Build configuration (Debug, Release, RelWithDebInfo, MinSizeRel)")

project(sample_rtree)
cmake_minimum_required(VERSION 2.6.0 FATAL_ERROR)


#include_directories(${CMAKE_SOURCE_DIR})
#link_directories(C:/lib/boost_1_60_0/lib/x64)

#set(BOOST_ROOT "C:/lib/boost_1_60_0")

include_directories(${CMAKE_SOURCE_DIR})

find_package(Boost 1.54 COMPONENTS system timer iostreams date_time filesystem)
include_directories(${Boost_INCLUDE_DIR})

# setup directory information for tclap (for argument parsing)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/tclap-1.2.1/include)

# setup directory informaiton for stxxl library
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/stxxl-1.4.1/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/stxxl-1.4.1/build/include)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/stxxl-1.4.1/build/lib)
# for debug library, use stxxl_debug
SET(STXXL_LIB stxxl)

# setup directory information for grpc library
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/grpc/include)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/grpc/libs/opt)
SET(GRPC_LIB gpr grpc grpc++ grpc_unsecure grpc++_unsecure)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/grpc/third_party/protobuf/src)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/grpc/libs/opt/protobuf)
SET(PROTOBUF_LIBRARIES protobuf)

# setup directory information for glog library
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/glog/src)
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/glog/.libs)
SET(GLOG_LIB glog)

SET(GOOG_LIB ${GRPC_LIB} ${PROTOBUF_LIBRARIES} ${GLOG_LIB})

# setup directory information for JSON SPIRIT
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/json_spirit_v4.08)

# setup protobuf target
SET(PROTOBUF_COMMAND_PATH ${CMAKE_SOURCE_DIR}/third_party/grpc/bins/opt/protobuf/)
SET(PROTO_FILE_PATH ${CMAKE_SOURCE_DIR}/server_code/protobuf/)
SET(PROTOBUF_PLUGIN_PATH ${CMAKE_SOURCE_DIR}/third_party/grpc/bins/opt/)
SET(PROTOBUF_PROTOS_LIB ${CMAKE_SOURCE_DIR}/third_party/grpc/src/python/grpcio_tests/tests/protoc_plugin/protos)

ADD_CUSTOM_COMMAND( OUTPUT server_code/protobuf/sampling_api.pb.cc server_code/protobuf/sampling_api.pb.h server_code/protobuf/sampling_api.grpc.pb.cc server_code/protobuf/sampling_api.grpc.pb.h
					PRE_BUILD
	                COMMAND ${PROTOBUF_COMMAND_PATH}/protoc --grpc_out=${PROTO_FILE_PATH}  --cpp_out=${PROTO_FILE_PATH} --plugin=protoc-gen-grpc=${PROTOBUF_PLUGIN_PATH}/grpc_cpp_plugin --proto_path=${PROTO_FILE_PATH} ${PROTO_FILE_PATH}/sampling_api.proto
					DEPENDS ${PROTO_FILE_PATH}/sampling_api.proto
					COMMENT " building sampling api"
			      )

ADD_CUSTOM_COMMAND( OUTPUT server_code/protobuf/sampling_api_pb2_grpc.py server_code/protobuf/sampling_api_pb2.py
                   PRE_BUILD
				   COMMAND python -m grpc_tools.protoc -I${PROTO_FILE_PATH} --python_out=${PROTO_FILE_PATH} --grpc_python_out=${PROTO_FILE_PATH} ${PROTO_FILE_PATH}/sampling_api.proto
				   DEPENDS ${PROTO_FILE_PATH}/sampling_api.proto
				   COMMENT " building sampling api python" )

#ADD_CUSTOM_COMMAND( OUTPUT server_code/protobuf/sampling_api_pb2.py
#					PRE_BUILD
#	                COMMAND ${PROTOBUF_COMMAND_PATH}/protoc --python_out=${PROTO_FILE_PATH} --plugin=protoc-gen-grpc=${PROTOBUF_PLUGIN_PATH}/grpc_python_plugin --proto_path=${PROTO_FILE_PATH} ${PROTO_FILE_PATH}/sampling_api.proto
#					DEPENDS ${PROTO_FILE_PATH}/sampling_api.proto
#					COMMENT " building python sampling api"
#			      )

ADD_CUSTOM_COMMAND( OUTPUT server_code/protobuf/server_state.pb.cc server_code/protobuf/server_state.pb.h
					PRE_BUILD
	                COMMAND ${PROTOBUF_COMMAND_PATH}/protoc                                --cpp_out=${PROTO_FILE_PATH}                                                                  --proto_path=${PROTO_FILE_PATH} ${PROTO_FILE_PATH}/server_state.proto
					DEPENDS ${PROTO_FILE_PATH}/server_state.proto
					COMMENT " building server state"
			      )


#find_package(Protobuf REQUIRED)
#include_directories(${PROTOBUF_INCLUDE_DIRS})

#EXEC_PROGRAM(gsl-config ARGS --cflags OUTPUT_VARIABLE GSL_CFLAGS)
#EXEC_PROGRAM(gsl-config ARGS --libs OUTPUT_VARIABLE GSL_LIBRARIES)

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -ggdb -gdwarf-3 -DDEBUG")
#set(CMAKE_CXX_FLAGS "-std=c++11")
# for profiling
set(CMAKE_CXX_FLAGS "-std=c++11 -O3 -fopenmp")
set(CMAKE_EXE_LINKER_FLAGS "-std=c++11")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GSL_CFLAGS}")

set(COMMON_SRC
    mongo_types.h
    mongo_types.cpp
   )

SET(SERVER_PROTO
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/protobuf/server_state.pb.cc
	server_code/protobuf/server_state.pb.h
	)

set(RTREE_SRC
    rtree/block_manager.h
    rtree/block_manager.cpp
    rtree/io_layers.h
    rtree/io_layers_impl.h
    rtree/naive_sample_query.h
    rtree/nodes.h
    rtree/nodes_impl.h
    rtree/range_reporter.h
    rtree/sample_builder.h
    rtree/sample_query.h
    rtree/node_loader.h
    rtree/rtree.h
    rtree/tests/integrity_checker.h
    level_sampling/level_sampling.h
    )

set(EXPERIMENT_SRC
	experiments/build_tree_experiments.cpp
	experiments/build_tree_experiments.h
	experiments/Data_Source_Information.cpp
	experiments/Data_Source_Information.h
	experiments/query_tree_experiments.cpp
	experiments/query_tree_experiments.h
	experiments/experiment_utilities.cpp
	experiments/experiment_utilities.h
	experiments/insert_and_delete_tree_experiments.cpp
	experiments/insert_and_delete_tree_experiments.h
	experiments/aggragate_experiments.cpp
	experiments/aggragate_experiments.h
	experiments/streaming_aggragate_query.cpp
	experiments/streaming_aggragate_query.h
	experiments/vary_sample_buffer.cpp
	experiments/vary_sample_buffer.h
    experiments/Independence_Experiment.cpp
    experiments/Independence_Experiment.h
	experiments/query_latency_experiment.cpp
	experiments/query_latency_experiment.h
	)
	
set(SERVER_SRC
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/protobuf/server_state.pb.cc
	server_code/protobuf/server_state.pb.h
	server_code/protobuf/sampling_api_pb2_grpc.py
	server_code/protobuf/sampling_api_pb2.py
	server_code/basic_types.h
	server_code/float_payload_types.h
	server_code/int_payload_types.h
	server_code/query_cursor.h
	server_code/query_cursor_repository.h
	server_code/query_cursor_repository.cpp
	server_code/sampling_structure.h
	server_code/sampling_structure_repository.h
	server_code/sampling_structure_repository.cpp
	server_code/RStree_basic.h
	server_code/RStree_basic.cpp
	server_code/query_cursor_basic.h
	server_code/query_cursor_basic.cpp
	server_code/Sampling_server.h
	server_code/Sampling_server.cpp
	server_code/Server_launcher.h
	server_code/Server_launcher.cpp
	server_code/server_main.cpp
    server_code/server_settings.h
    server_code/server_settings.cpp
	)

set(SERVER_CLI_SRC
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/Server_cli.cpp
	)
	
set(SERVER_TEST_SRC
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/server_rpc_tests.cpp
	)
	
set(SERVER_BUILD_SRC
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/build_command_main.cpp
)

set(SERVER_QUERY_SRC
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/query_command_main.cpp
)

set(SERVER_DUMP_MONGO_SRC
	server_code/protobuf/sampling_api.pb.h
	server_code/protobuf/sampling_api.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.cc
	server_code/protobuf/sampling_api.grpc.pb.h
	server_code/dump_mongo_command_main.cpp
)

add_executable(exp ${COMMON_SRC} ${RTREE_SRC} ${EXPERIMENT_SRC} exp.cpp)
target_link_libraries(exp ${Boost_LIBRARIES} ${GSL_LIBRARIES} ${Boost_LIBRARIES} ${STXXL_LIB} pthread)

#add_executable(exp_latency ${COMMON_SRC} ${RTREE_SRC} exp_latency.cpp)
#target_link_libraries(exp_latency ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES} tpie)

#add_executable(exp_aggregation ${COMMON_SRC} ${RTREE_SRC} exp_aggregation.cpp)
#target_link_libraries(exp_aggregation ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(exp_query_size ${COMMON_SRC} ${RTREE_SRC} exp_query_size.cpp)
#target_link_libraries(exp_query_size ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(calc_tree_size ${COMMON_SRC} ${RTREE_SRC} calc_tree_size.cpp)
#target_link_libraries(calc_tree_size ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(test ${COMMON_SRC} ${RTREE_SRC} test.cpp)
#target_link_libraries(test ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(test2 ${COMMON_SRC} ${RTREE_SRC} test2.cpp)
#target_link_libraries(test2 ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(generate_data ${COMMON_SRC} ${RTREE_SRC} generate_data.cpp)
#target_link_libraries(generate_data ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

add_executable(sample_server ${COMMON_SRC} ${RTREE_SRC} ${SERVER_SRC})
target_link_libraries(sample_server ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)

add_executable(sample_server_test ${SERVER_TEST_SRC})
target_link_libraries(sample_server_test ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)

#add_executable(test_random_shuffle random_shuffle.h test_random_shuffle.cpp)
#target_link_libraries(test_random_shuffle ${Boost_LIBRARIES} ${TPIE_LIBRARIES} ${GSL_LIBRARIES})

#add_executable(test_disk_read test_disk_read.cpp)
#target_link_libraries(test_disk_read ${Boost_LIBRARIES})

#add_executable(test_random_number test_random_number.cpp)
#target_link_libraries(test_random_number ${Boost_LIBRARIES} ${GSL_LIBRARIES})

add_executable(sample_server_cli ${SERVER_CLI_SRC})
target_link_libraries(sample_server_cli ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)
	
add_executable(sample_server_build ${SERVER_BUILD_SRC})
target_link_libraries(sample_server_build ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)

add_executable(sample_server_dump_mongo ${SERVER_DUMP_MONGO_SRC})
target_link_libraries(sample_server_dump_mongo ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)

add_executable(sample_server_query ${SERVER_QUERY_SRC} ${SERVER_PROTO})
target_link_libraries(sample_server_query ${Boost_LIBRARIES} ${GOOG_LIB} ${GSL_LIBRARIES} ${STXXL_LIB} dl)
