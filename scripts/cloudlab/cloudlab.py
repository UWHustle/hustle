import json
import sys
import subprocess
import datetime
import csv
import os

time_format = "%Y-%m-%d %H:%M:%S"

REPO_DIR = "/mydata/repo/"
RESULT_DIR = "/mydata/results/"
HUSTLE_BUILD_DIR = "build_release/"
SSB_BENCHMARK_DIR = REPO_DIR + HUSTLE_BUILD_DIR + "src/benchmark/"
SSB_BENCHMARK_PATH = SSB_BENCHMARK_DIR + "hustle_src_benchmark_main"
SSB_BENCHMARK_DEFAULT_ARGS = ["ssb", "hash-aggregate"]

os.chdir(SSB_BENCHMARK_DIR)


def parse_bench_out(a_experiment_num, str_output):
    out = []
    str_experiment_num = str(a_experiment_num)  # allow inputs that convert to string
    str_arr = str_output.split("\\n")
    # sample line:
    # query11      41149293 ns      2607978 ns          100
    for a_str in str_arr:
        if a_str.find("query") != -1:
            str_list = a_str.split()  # split on all whitespace
            if len(str_list) == 6:
                query = str_list[0]
                time_ns = str_list[1]
                cpu_ns = str_list[3]
                iterations = str_list[5]
                out.append((
                    experiment_num,
                    query,
                    time_ns,
                    cpu_ns,
                    iterations
                ))
    return out


if __name__ == '__main__':
    print(datetime.datetime.now().strftime(time_format) + " | Starting benchmarking script...")
    print(datetime.datetime.now().strftime(time_format) + " | Loading parameters...")
    json_str = sys.argv[1]
    args = json.loads(json_str)
    experiments = [
        args["experiment_1_flags"],
        args["experiment_2_flags"],
        args["experiment_3_flags"],
        args["experiment_4_flags"],
        args["experiment_5_flags"],
    ]
    print(datetime.datetime.now().strftime(time_format) + " | Starting Experiments...")
    experiment_results = []
    print(datetime.datetime.now().strftime(time_format) + " | Starting default experiment...")
    default_command = [SSB_BENCHMARK_PATH]
    default_command.extend(SSB_BENCHMARK_DEFAULT_ARGS)
    process = subprocess.Popen(default_command, shell=True)
    process.wait()
    proc_out, proc_err = process.communicate()
    experiment_results.append(parse_bench_out("default", str(proc_out, 'utf-8')))
    print(datetime.datetime.now().strftime(time_format) + " | Starting numbered experiments...")
    for experiment_flags, experiment_num in zip(experiments, [1, 2, 3, 4, 5]):
        if experiment_flags != "":
            flag_list = experiment_flags.split(" ")
            command = [SSB_BENCHMARK_PATH]
            command.extend(SSB_BENCHMARK_DEFAULT_ARGS)
            command.extend(flag_list)
            print(
                datetime.datetime.now().strftime(time_format) + " | Starting Experiment #" + str(experiment_num) + "..."
            )
            print(datetime.datetime.now().strftime(time_format) + " | Experiment Flags: " + experiment_flags)
            process = subprocess.Popen(command, shell=True)
            process.wait()
            proc_out, proc_err = process.communicate()
            experiment_results.append(parse_bench_out(experiment_num, str(proc_out, 'utf-8')))
    print(datetime.datetime.now().strftime(time_format) + " | Experiments finished.")
    print(datetime.datetime.now().strftime(time_format) + " | Saving results...")
    with open(RESULT_DIR + "report.csv") as a_file:
        csv_writer = csv.writer(a_file)
        csv_writer.writerow(("experiment number", "query", "time (ns)", "cpu time (ns)", "iterations"))
        for a_result in experiment_results:
            for a_line in a_result:
                csv_writer.writerow(a_line)
    print(datetime.datetime.now().strftime(time_format) + " | Results saved to .")
