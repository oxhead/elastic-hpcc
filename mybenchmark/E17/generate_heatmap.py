import itertools

from elastic.hpcc import placement

from mybenchmark.base import config as myconfig
from mybenchmark.base import *
from mybenchmark.E14 import myplacement

def main(run_id):
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_4thor_4roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E17')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_8nodes_direct.yaml"))
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 240)
    default_setting.set_config('workload.dispatch_mode', 'once')
    default_setting.set_config('workload.type', 'constant')
    #default_setting.set_config('workload.distribution', {"type": "uniform"})
    #default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    #default_setting.set_config('workload.selection', {"type": "uniform"})
    default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})

    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'],
        "placement2"
    )

    workload_distribution_list = {
        "beta": {"type": "beta", "alpha": 2, "beta": 5},
        "gamma": {"type": "gamma", "shape": 2},
        "normal": {"type": "normal"},
        "powerlaw": {"type": "powerlaw", "shape": 5},
        "uniform": {"type": "uniform"},
    }

    variable_setting_list = []
    for workload_name, workload_config in workload_distribution_list.items():
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, workload_name),
            'workload.distribution': workload_config,
        })

    os.system("mkdir -p {}/placement".format(script_dir))
    for experiment in generate_experiments(default_setting, variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
        #print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        #break
        experiment.run()
        os.system("cp {}/result/access_distribution.json {}/placement/{}.json".format(experiment.output_dir, script_dir, experiment.workload_config.lookup_config('workload.distribution.type')))
        #pass

if __name__ == "__main__":
    # repeat how many times
    for run_id in range(1, 2):
        main(run_id)
