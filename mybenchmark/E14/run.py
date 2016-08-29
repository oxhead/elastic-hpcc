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
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_4thor_8roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E14')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template_8nodes_direct.yaml"))
    default_setting.set_config('experiment.benchmark_clients', 8)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 120)
    default_setting.set_config('workload.type', 'constant')
    #default_setting.set_config('workload.distribution', {"type": "uniform"})
    #default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    #default_setting.set_config('workload.selection', {"type": "uniform"})
    #default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})
    default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})

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

    data_placement_list = {
        "beta": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R,os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw5.json")),
        "uniform": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R,os.path.join(script_dir, "placement", "uniform.json")),
    }
    #data_placement_list = {
    #    "beta": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
    #    "gamma": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
    #    "normal": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "normal.json")),
    #    "powerlaw": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw5.json")),
    #    "uniform": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "uniform.json")),
    #}
    workload_distribution_list = {
        "beta": {"type": "beta", "alpha": 2, "beta": 5},
        "gamma": {"type": "gamma", "shape": 2},
        "normal": {"type": "normal"},
        "powerlaw": {"type": "powerlaw", "shape": 5},
        "uniform": {"type": "uniform"},
    }

    variable_setting_list = []
    for workload, data_placement in itertools.product(workload_distribution_list.items(), data_placement_list.items()):
        workload_name, workload_config = workload
        data_placement_name, data_placement_profile = data_placement
        if (workload_name == "uniform") and (data_placement_name == "uniform"):
            pass
        elif workload_name == data_placement_name:
            continue
        else:
            continue
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, 'w-{}_dp-{}_{}'.format(workload_name, data_placement_name, run_id)),
            'experiment.data_placement': data_placement_profile,
            'workload.distribution': workload_config,
        })

    for experiment in generate_experiments(default_setting, variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1):
        #print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        experiment.run()
        #break  # test just the first one
        #import time
        #time.sleep(10)
        #pass


if __name__ == "__main__":
    # repeat how many times
    for run_id in range(2, 3):
        main(run_id)
