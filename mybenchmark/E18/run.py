import itertools

from elastic.hpcc import placement

from mybenchmark.base import config as myconfig
from mybenchmark.base import *
from mybenchmark.E18 import myplacement

def main(run_id, dp_type, selective_run):
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    #####################################
    # experiment setting
    #####################################
    # 1. workload parameters
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_4thor_8roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E18')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template.yaml"))
    default_setting.set_config('experiment.workload_endpoints', 4)
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    # this only uses one query application
    default_setting.set_config('experiment.applications', ['sequential_search_firstname', 'sequential_search_lastname', 'sequential_search_city', 'sequential_search_zip'])
    default_setting.set_config('workload.num_queries', 100)
    default_setting.set_config('workload.period', 60)
    default_setting.set_config('workload.dispatch_mode', 'once')
    default_setting.set_config('workload.type', 'constant')
    # default_setting.set_config('workload.distribution', {"type": "uniform"})
    # default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    # default_setting.set_config('workload.selection', {"type": "uniform"})
    # default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})

    # 2. pick a data placement kind -> [coarse|fine]
    data_placement_type = dp_type

    # 3. output directory
    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'],
        data_placement_type
    )

    #####################################
    # experiment setting
    #####################################
    coarse_data_placement_list = {
        "beta": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw.json")),
        "uniform": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R,os.path.join(script_dir, "placement", "uniform.json")),
    }
    fine_data_placement_list = {
        "beta": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw.json")),
        "uniform": (placement.DataPlacementType.fine_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "uniform.json")),
    }
    data_placement_list = coarse_data_placement_list if data_placement_type == 'coarse' else fine_data_placement_list

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
        # selective evaluations for shortening process
        #if not workload_name == 'powerlaw':
        #    continue
        #if not data_placement_name == 'uniform':
        #    continue
        #if selective_run:
        #    if not ((workload_name == data_placement_name) or data_placement_name == "uniform"):
        #        continue
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, 'w-{}_dp-{}_{}'.format(workload_name, data_placement_name, run_id)),
            'experiment.data_placement': data_placement_profile,
            'workload.distribution': workload_config,
        })

    for experiment in generate_experiments(default_setting, variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1, check_success=True):
        #print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        experiment.run()


if __name__ == "__main__":
    for run_id in range(1, 4):
        #for dp_type in ['fine', 'coarse']:
        for dp_type in ['coarse']:
            main(run_id, dp_type, False)
