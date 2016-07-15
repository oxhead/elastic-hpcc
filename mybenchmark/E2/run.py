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
        'experiment_id': 'E2',
        'arrival_type': 'constant',
        'application': 'bm1',
        'num_queries': 100,
        'period': 120,
        'workload_file': os.path.join(script_dir, "workload_template3.yaml")
    }

    output_dir = os.path.join(myconfig['result_dir'], default_setting['experiment_id'], "layout_1r")
    variable_setting = [
        {
            'workload_file': os.path.join(script_dir, "workload_template3.yaml"),
            'distribution': {"type": "uniform"},
            'output_dir': os.path.join(output_dir, 'single')
        },
        {
            'workload_file': os.path.join(script_dir, "workload_template4.yaml"),
            'distribution': {"type": "uniform"},
            'output_dir': os.path.join(output_dir, 'uniform')
        },
        {
            'workload_file': os.path.join(script_dir, "workload_template4.yaml"),
            'distribution': {"type": "pareto", "alpha": 3},
            'output_dir': os.path.join(output_dir, 'pareto')
        }
    ]

    compare_runs(default_setting, variable_setting)


if __name__ == "__main__":
    main()
