from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main():
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/ubuntu/elastic-hpcc/template/elastic_4roxie.xml")
    default_setting.set_config('cluster.benchmark', "/home/ubuntu/elastic-hpcc/conf/4driver.yaml")
    default_setting.set_config('experiment.id', 'E4')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_direct.yaml"))
    default_setting.set_config('workload.type', 'constant')
    default_setting.set_config('workload.num_queries', 100)
    default_setting.set_config('workload.period', 120)

    output_dir = os.path.join(default_setting['experiment.result_dir'], default_setting['experiment.id'], "layout_1r_{}q_{}sec".format(default_setting['workload.num_queries'], default_setting['workload.period']))
    run_ids = range(1, 11)
    for run_id in run_ids:
        for alpha in [10]:
        #for alpha in range(1, 12, 3):
            variable_setting = [
                {
                    'experiment.application': 'bm1_distribution',
                    'experiment.output_dir': os.path.join(output_dir, 'pareto_alpha{}_{}'.format(alpha, run_id)),
                    'workload.distribution': {"type": "pareto", "alpha": alpha},
                },
            ]

            for experiment in generate_experiments(default_setting, variable_setting, experiment_dir=script_dir, timeline_reuse=False, wait_time=1):
                experiment.run()

if __name__ == "__main__":
    main()
