sudo DORADD_RESULTS_FILE=/tmp/test.txt taskset -c 4-11 ./app/build/ycsb -n 8 ./app/ycsb/gen-log/ycsb_uniform_no_cont.txt -i exp:4000
