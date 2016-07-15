from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")

    # cluster setting
    hpcc_cluster = HPCCCluster.parse_config("/home/ubuntu/elastic-hpcc/template/elastic_4roxie.xml")
    benchmark_config = BenchmarkConfig.parse_file("/home/ubuntu/elastic-hpcc/conf/4driver.yaml")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    default_setting = {
        'hpcc_cluster': hpcc_cluster,
        'benchmark_config': benchmark_config,
        'experiment_id': 'test',
        'arrival_type': 'constant',
        'application': 'bm1',
        'workload.num_queries': 10,
        'workload.period': 60,
    }

    output_dir = os.path.join(myconfig['result_dir'], default_setting['experiment_id'], "test1")
    variable_setting = [
        {
            'workload_file': os.path.join(script_dir, "workload_single.yaml"),
            'output_dir': os.path.join(output_dir, 'single')
        },
        {
            'workload_file': os.path.join(script_dir, "workload.yaml"),
            'output_dir': os.path.join(output_dir, 'pareto')
        },
        {
            'workload_file': os.path.join(script_dir, "workload_direct_single.yaml"),
            'output_dir': os.path.join(output_dir, 'direct')
        }
    ]

    compare_runs(default_setting, variable_setting[1:2])


if __name__ == "__main__":
    main()
