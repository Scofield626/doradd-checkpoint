cmake_minimum_required(VERSION 3.8)
project(doradd-bench CXX)

set(VERONA_PATH "../external/verona-rt")
set(SNMALLOC_PATH "../external/snmalloc/")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wno-pointer-arith)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Release Build
if(CMAKE_BUILD_TYPE MATCHES Release)
  set(BRANCH "release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  # show symbols in perf top (assembly)
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
endif()

# Debug Build
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(BRANCH "debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
  #set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -static-libasan")
  #set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address")
endif()

find_package(Threads REQUIRED)

# Set default checkpoint batch size if not defined
if(NOT DEFINED CHECKPOINT_BATCH_SIZE)
  set(CHECKPOINT_BATCH_SIZE 1)
endif()

# Set default checkpoint threshold if not defined
if(NOT DEFINED CHECKPOINT_THRESHOLD)
  set(CHECKPOINT_THRESHOLD 100000)
endif()

# Set default checkpoint db path if not defined
if(NOT DEFINED CHECKPOINT_DB_PATH)
  set(CHECKPOINT_DB_PATH "checkpoint.db")
endif()

# YCSB
add_executable(ycsb ycsb.cc)
add_compile_definitions(PREFETCH)
add_compile_definitions(CORE_PIPE)
add_compile_definitions(INDEXER)
# add_compile_definitions(RPC_LATENCY)
# add_compile_definitions(LOG_LATENCY)
#add_compile_definitions(LOG_SCHED_OHEAD)
#add_compile_definitions(ZERO_SERV_TIME)
#add_compile_definitions(TEST_TWO)

# Add CHECKPOINT_BATCH_SIZE and CHECKPOINT_THRESHOLD as compile definitions
target_compile_definitions(ycsb PRIVATE CHECKPOINT_BATCH_SIZE=${CHECKPOINT_BATCH_SIZE})
target_compile_definitions(ycsb PRIVATE CHECKPOINT_THRESHOLD=${CHECKPOINT_THRESHOLD})
target_compile_definitions(ycsb PRIVATE "CHECKPOINT_DB_PATH=\"${CHECKPOINT_DB_PATH}\"")

target_include_directories(ycsb PRIVATE ../src/misc)
target_include_directories(ycsb PRIVATE ../src/doradd)
target_include_directories(ycsb PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(ycsb PRIVATE ${SNMALLOC_PATH}/src)
target_compile_options(ycsb PRIVATE -mcx16 -march=native)
target_compile_definitions(ycsb PRIVATE -DSNMALLOC_CHEAP_CHECKS)
target_compile_definitions(ycsb PRIVATE -DACQUIRE_ALL)
target_link_libraries(ycsb PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(ycsb PRIVATE atomic)
target_include_directories(ycsb PRIVATE ../external/SPSCQueue/include/rigtorp)

# Find RocksDB
find_library(ROCKSDB_LIBRARY NAMES rocksdb)
find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)

if(NOT ROCKSDB_LIBRARY OR NOT ROCKSDB_INCLUDE_DIR)
    message(FATAL_ERROR "RocksDB not found. Please install librocksdb-dev")
endif()

# Add RocksDB include directories and library
target_include_directories(ycsb PRIVATE ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(ycsb PRIVATE ${ROCKSDB_LIBRARY})
target_link_libraries(ycsb PRIVATE -lgflags -lsnappy -lz -lbz2 -llz4 -lzstd)
#target_link_libraries(ycsb PRIVATE -labsl_hash -labsl_raw_hash_set)

# Commenting out all TPCC-related sections
#add_custom_command(
#  OUTPUT ${CMAKE_SOURCE_DIR}/tpcc_gen.cc
#  COMMAND m4 ${CMAKE_SOURCE_DIR}/tpcc_macros.m4 ${CMAKE_SOURCE_DIR}/tpcc.cc > ${CMAKE_SOURCE_DIR}/tpcc_gen.cc
#  DEPENDS ${CMAKE_SOURCE_DIR}/tpcc_macros.m4 ${CMAKE_SOURCE_DIR}/tpcc.cc
#  COMMENT "Generating tpcc_gen.cc"
#  VERBATIM
#)

# Commenting out TPCC executable and its configurations
#add_executable(tpcc ${CMAKE_SOURCE_DIR}/tpcc_gen.cc)

#add_compile_definitions(SINGLE_TABLE)
#add_compile_definitions(WAREHOUSE_SPLIT)

#target_include_directories(tpcc PRIVATE ../src/misc)
#target_include_directories(tpcc PRIVATE ../src/doradd)
#target_include_directories(tpcc PRIVATE ${VERONA_PATH}/src/rt)
#target_include_directories(tpcc PRIVATE ${SNMALLOC_PATH}/src)
#target_compile_options(tpcc PRIVATE -mcx16 -march=native)
#target_compile_definitions(tpcc PRIVATE -DSNMALLOC_CHEAP_CHECKS)
#target_compile_definitions(tpcc PRIVATE -DACQUIRE_ALL)
#target_link_libraries(tpcc PRIVATE ${CMAKE_THREAD_LIBS_INIT})
#target_link_libraries(tpcc PRIVATE atomic)
#target_include_directories(tpcc PRIVATE ../external/SPSCQueue/include/rigtorp)

# Find Google Test
find_package(GTest REQUIRED)

# RocksDB Test
add_executable(rocksdb_test rocksdb_test.cc)
target_include_directories(rocksdb_test PRIVATE ../src/misc)
target_include_directories(rocksdb_test PRIVATE ../src/doradd)
target_include_directories(rocksdb_test PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(rocksdb_test PRIVATE ${SNMALLOC_PATH}/src)
target_compile_options(rocksdb_test PRIVATE -mcx16 -march=native)
target_link_libraries(rocksdb_test PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(rocksdb_test PRIVATE atomic)
target_include_directories(rocksdb_test PRIVATE ${ROCKSDB_INCLUDE_DIR})
target_link_libraries(rocksdb_test PRIVATE ${ROCKSDB_LIBRARY})
target_link_libraries(rocksdb_test PRIVATE -lgflags -lsnappy -lz -lbz2 -llz4 -lzstd)
target_link_libraries(rocksdb_test PRIVATE GTest::GTest GTest::Main)

# Checkpointer Test
# add_executable(checkpointer_test checkpointer_test.cc)
# target_include_directories(checkpointer_test PRIVATE ../src/misc)
# target_include_directories(checkpointer_test PRIVATE ../src/doradd)
# target_include_directories(checkpointer_test PRIVATE ../src/storage)
# target_include_directories(checkpointer_test PRIVATE ${VERONA_PATH}/src/rt)
# target_include_directories(checkpointer_test PRIVATE ${SNMALLOC_PATH}/src)
# target_compile_options(checkpointer_test PRIVATE -mcx16 -march=native)
# target_link_libraries(checkpointer_test PRIVATE ${CMAKE_THREAD_LIBS_INIT})
# target_link_libraries(checkpointer_test PRIVATE atomic)
# target_include_directories(checkpointer_test PRIVATE ${ROCKSDB_INCLUDE_DIR})
# target_link_libraries(checkpointer_test PRIVATE ${ROCKSDB_LIBRARY})
# target_link_libraries(checkpointer_test PRIVATE -lgflags -lsnappy -lz -lbz2 -llz4 -lzstd)
# target_link_libraries(checkpointer_test PRIVATE GTest::GTest GTest::Main)
# target_include_directories(checkpointer_test PRIVATE ../external/SPSCQueue/include/rigtorp)

# Pipeline Test
# add_executable(pipeline_test pipeline_test.cc)
# target_include_directories(pipeline_test PRIVATE ../src/misc)
# target_include_directories(pipeline_test PRIVATE ../src/doradd)
# target_include_directories(pipeline_test PRIVATE ../src/storage)
# target_include_directories(pipeline_test PRIVATE ${VERONA_PATH}/src/rt)
# target_include_directories(pipeline_test PRIVATE ${SNMALLOC_PATH}/src)
# target_compile_options(pipeline_test PRIVATE -mcx16 -march=native)
# target_link_libraries(pipeline_test PRIVATE ${CMAKE_THREAD_LIBS_INIT})
# target_link_libraries(pipeline_test PRIVATE atomic)
# target_include_directories(pipeline_test PRIVATE ${ROCKSDB_INCLUDE_DIR})
# target_link_libraries(pipeline_test PRIVATE ${ROCKSDB_LIBRARY})
# target_link_libraries(pipeline_test PRIVATE -lgflags -lsnappy -lz -lbz2 -llz4 -lzstd)
# target_link_libraries(pipeline_test PRIVATE GTest::GTest GTest::Main)
# target_include_directories(pipeline_test PRIVATE ../external/SPSCQueue/include/rigtorp)


