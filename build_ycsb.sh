cd app/
mkdir -p build && cd build
mkdir -p results # collect stats
sudo rm -rf ./checkpoint.db/
ninja clean
cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-DRPC_LATENCY -DLOG_LATENCY" -DCHECKPOINT_BATCH_SIZE=32 -DCHECKPOINT_THRESHOLD=150000000 -DCHECKPOINT_DB_PATH="checkpoint.db"
ninja
