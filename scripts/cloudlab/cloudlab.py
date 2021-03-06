import json
import pprint
import urllib.parse
import subprocess
import datetime
import csv
import os

pprinter = pprint.PrettyPrinter(indent=2)

time_format = "%Y-%m-%d %H:%M:%S"

PARAMS_FILE = "/mydata/params.json"
REPO_DIR = "/mydata/repo/"
RESULT_DIR = "/mydata/results/"
HUSTLE_BUILD_DIR = "build_release/"
SSB_BENCHMARK_DIR = REPO_DIR + HUSTLE_BUILD_DIR + "src/benchmark/"
SSB_BENCHMARK_PATH = SSB_BENCHMARK_DIR + "hustle_src_benchmark_main"

os.chdir(SSB_BENCHMARK_DIR)


def parse_bench_out(a_experiment_num, str_output):
    out = []
    str_experiment_num = str(a_experiment_num)  # allow inputs that convert to string
    str_arr = str_output.split("\n")
    # sample line:
    # query11      41149293 ns      2607978 ns          100
    found_output = False
    for a_str in str_arr:
        if found_output:
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
        else:
            if a_str.find("------") != -1:
                found_output = True
    return out


def safe_str_enc(a_str):
    if a_str is None:
        return ''
    else:
        return str(a_str, 'utf-8')


if __name__ == '__main__':
    print(datetime.datetime.now().strftime(time_format) + " | Starting Automated Cloudlab Benchmark.")
    print(datetime.datetime.now().strftime(time_format) + " | Loading parameters...")
    with open(PARAMS_FILE, 'r') as json_file:
        args = json.loads(urllib.parse.unquote_plus(json_file.readline()))
    print(datetime.datetime.now().strftime(time_format) + " | Parameters loaded. Printing: ")
    pprinter.pprint(args)
    experiments = [
        args["experiment_1_args"],
        args["experiment_2_args"],
        args["experiment_3_args"],
        args["experiment_4_args"],
        args["experiment_5_args"],
    ]
    print(datetime.datetime.now().strftime(time_format) + " | Machine Parameters:")
    print(datetime.datetime.now().strftime(time_format) + " | Hardware: " + str(args['hardware']))
    print(datetime.datetime.now().strftime(time_format) + " | Storage Size: " + str(args['storage']))
    print(datetime.datetime.now().strftime(time_format) + " | scale_factor: " + str(args['scale_factor']))
    print(datetime.datetime.now().strftime(time_format) + " | Starting benchmarking script...")
    print(datetime.datetime.now().strftime(time_format) + " | Starting Experiments...")
    experiment_results = []
    print(datetime.datetime.now().strftime(time_format) + " | Starting numbered experiments...")
    for experiment_args, experiment_num in zip(experiments, [1, 2, 3, 4, 5]):
        if experiment_args != "skip":
            command = [SSB_BENCHMARK_PATH]
            if str(args['common_args']) != 'skip':
                command.extend(str(args['common_args']).split(" "))
            command.extend(experiment_args.split(" "))
            print(
                datetime.datetime.now().strftime(time_format) + " | Starting Experiment #" + str(experiment_num) + "..."
            )
            print(datetime.datetime.now().strftime(time_format) + " | Experiment Command: " + ' '.join(command))
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            proc_out, proc_err = process.communicate()
            proc_out_str = safe_str_enc(proc_out)
            proc_err_str = safe_str_enc(proc_err)
            print(datetime.datetime.now().strftime(time_format) + " | Experiment Output:")
            print(proc_out_str)
            print(datetime.datetime.now().strftime(time_format) + " | Experiment Error:")
            print(proc_err_str)
            print(datetime.datetime.now().strftime(time_format) + " | Experiment Return Code:")
            print(process.returncode)
            experiment_results.append(parse_bench_out(experiment_num, proc_out_str))
        else:
            print(
                datetime.datetime.now().strftime(time_format) + " | Experiment #" + str(experiment_num) + " skipped."
            )
    print(datetime.datetime.now().strftime(time_format) + " | Experiments finished.")
    print(datetime.datetime.now().strftime(time_format) + " | Saving results...")
    with open(RESULT_DIR + "report.csv", 'w', newline='') as a_file:
        csv_writer = csv.writer(a_file)
        csv_writer.writerow(("experiment number", "query", "time (ns)", "cpu time (ns)", "iterations"))
        for a_result in experiment_results:
            for a_line in a_result:
                csv_writer.writerow(a_line)
    print(datetime.datetime.now().strftime(time_format) + " | Results saved to: \"/mydata/results/report.csv\"")
