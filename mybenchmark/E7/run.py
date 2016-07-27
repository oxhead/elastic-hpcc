from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/ubuntu/elastic-hpcc/template/elastic_8roxie.xml")
    default_setting.set_config('cluster.benchmark', "/home/ubuntu/elastic-hpcc/conf/10driver.yaml")
    default_setting.set_config('experiment.id', 'E7')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_8nodes_direct.yaml"))
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 120)
    default_setting.set_config('workload.type', 'constant')
    default_setting.set_config('workload.distribution', {"type": "uniform"})
    default_setting.set_config('workload.selection', {"type": "uniform"})


    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'], "nolocality_10bm_48threads_8roxie_60threads_layout_1r_{}q_{}sec".format(
            default_setting['workload.num_queries'],
            default_setting['workload.period']
        )
    )

    run_ids = range(1, 4)
    for run_id in run_ids:
        variable_setting = [
            {
                # 'experiment.applications': ['random_search_firstname'],
                'experiment.output_dir': os.path.join(output_dir, 'powerlaw5_{}'.format(run_id)),
                'workload.selection': {"type": "powerlaw", "shape": 5},
            },
            {
                #'experiment.applications': ['random_search_firstname'],
                'experiment.output_dir': os.path.join(output_dir, 'uniform_{}'.format(run_id)),
                'workload.selection': {"type": "uniform"},
            },
        ]

        for experiment in generate_experiments(default_setting, variable_setting[:], experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
            experiment.run()
            #pass


if __name__ == "__main__":
    main()
