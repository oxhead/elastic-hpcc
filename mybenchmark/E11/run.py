from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_1thor_4roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/4driver.yaml")
    default_setting.set_config('experiment.id', 'E11')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_4nodes_direct.yaml"))
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 5)
    default_setting.set_config('workload.type', 'constant')
    default_setting.set_config('workload.distribution', {"type": "uniform"})
    #default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    default_setting.set_config('workload.selection', {"type": "uniform"})


    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'], "official2_64+32threads_full8_8N_8R_{}q_{}sec".format(
            default_setting['workload.num_queries'],
            default_setting['workload.period']
        )
    )

    run_ids = range(1, 2)
    for run_id in run_ids:
        variable_setting = [
            {
                'experiment.applications': ['sequential_search_firstname'],
                'experiment.output_dir': os.path.join(output_dir, 'uniform_{}'.format(run_id)),
            },
        ]

        for experiment in generate_experiments(default_setting, variable_setting, experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
            experiment.run()
            #pass


if __name__ == "__main__":
    main()
