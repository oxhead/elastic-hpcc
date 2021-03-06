from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_4thor_8roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E13')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_8nodes_direct.yaml"))
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    default_setting.set_config('workload.num_queries', 300)
    default_setting.set_config('workload.period', 120)
    default_setting.set_config('workload.type', 'constant')
    #default_setting.set_config('workload.distribution', {"type": "uniform"})
    #default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    default_setting.set_config('workload.selection', {"type": "uniform"})


    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'], "{}rthreads_{}bmclients_{}bmthreads_full8_8N_xR_{}q_{}sec".format(
            default_setting['experiment.roxie_concurrency'],
            default_setting['experiment.benchmark_clients'],
            default_setting['experiment.benchmark_concurrency'],
            default_setting['workload.num_queries'],
            default_setting['workload.period']
        )
    )

    run_ids = range(1, 10)
    for run_id in run_ids:

        variable_setting = [
            {
                'experiment.applications': ['sequential_search_firstname'],
                'experiment.output_dir': os.path.join(output_dir, 'uniform_{}'.format(run_id)),
                'workload.distribution': {"type": "uniform"},
            },
            {
                'experiment.applications': ['sequential_search_firstname'],
                'experiment.output_dir': os.path.join(output_dir, 'powerlaw5_{}'.format(run_id)),
                'workload.distribution': {"type": "powerlaw", "shape": 5},
            }
        ]

        for experiment in generate_experiments(default_setting, variable_setting[1:], experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
            experiment.run()
            import time
            time.sleep(120)
            #pass


if __name__ == "__main__":
    main()
