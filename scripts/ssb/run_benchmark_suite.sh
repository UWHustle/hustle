cur_dir=`dirname $0`

for i in 16 32 64 128 256; do
    echo Run benchmark on $i scale factor
    sh ${cur_dir}/gen_benchmark_data.sh $i
    sh ${cur_dir}/run_benchmark.sh hash_aggregate| tee "result_ssb_$(date '+%Y%m%d_%H%M%S')_hash_$i.txt"
	sh ${cur_dir}/run_benchmark.sh arrow_aggregate| tee "result_ssb_$(date '+%Y%m%d_%H%M%S')_arrow_$i.txt"
done
