from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/ubuntu/elastic-hpcc/template/elastic_8roxie.xml")
    default_setting.set_config('cluster.benchmark', "/home/ubuntu/elastic-hpcc/conf/10driver.yaml")
    default_setting.set_config('experiment.id', 'E6')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_8nodes_direct.yaml"))
    default_setting.set_config('workload.type', 'poisson')
    default_setting.set_config('workload.num_queries', 400)
    default_setting.set_config('workload.period', 120)

    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'], "10bm_48threads_8roxie_60threads_layout_1r_{}q_{}sec".format(
            default_setting['workload.num_queries'],
            default_setting['workload.period']
        )
    )

    run_ids = range(2, 4)
    for run_id in run_ids:
        variable_setting = [
            {
                'experiment.application': 'random_search_firstname_distribution',
                'experiment.output_dir': os.path.join(output_dir, 'randomsearch_pareto_{}'.format(run_id)),
                'workload.distribution': {"type": "pareto", "alpha": 3},
            },
            {
                'experiment.application': 'random_search_firstname_distribution',
                'experiment.output_dir': os.path.join(output_dir, 'randomsearch_uniform_{}'.format(run_id)),
                'workload.distribution': {"type": "uniform"},
            },
        ]

        for experiment in generate_experiments(default_setting, variable_setting, experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
            experiment.run()

if __name__ == "__main__":
    main()
