import itertools

from elastic.hpcc import placement

from mybenchmark.base import config as myconfig
from mybenchmark.base import *
from mybenchmark.E23 import myplacement

def main(run_id, dp_type, dp_model, selective_run, check_success):
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    #####################################
    # experiment setting
    #####################################
    # 1. workload parameters
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_4thor_8roxie_locality.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E23')
    default_setting.set_config('experiment.goal', '''
    1. Reduce query response time
    ''')
    default_setting.set_config('experiment.conclusion', '''
        1. Seems 4 driver nodes and 16 threads per node is enough to drive the maximum throughput
        ''')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template.yaml"))
    default_setting.set_config('experiment.workload_endpoints', 4)
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 16)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    # this only uses one query application
    #default_setting.set_config('experiment.applications', ['sequential_search_firstname', 'sequential_search_lastname', 'sequential_search_city', 'sequential_search_zip'])
    default_setting.set_config('workload.num_queries', 100)
    default_setting.set_config('workload.period', 300)
    default_setting.set_config('workload.dispatch_mode', 'once')
    default_setting.set_config('workload.type', 'constant')
    # default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})

    # 2. pick a data placement kind -> [coarse|fine]
    data_placement_type = dp_type

    # 3. output directory
    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'],
        "{}-{}".format(data_placement_type, dp_model)
    )

    #####################################
    # experiment setting
    #####################################
    coarse_data_placement_list = {
        "beta": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw.json")),
        "uniform": (placement.DataPlacementType.coarse_partial, myplacement.DP_1R, os.path.join(script_dir, "placement", "uniform.json")),
        "complete": (placement.DataPlacementType.complete, myplacement.DP_1R, os.path.join(script_dir, "placement", "uniform.json")),
    }
    fine_data_placement_list = {
        "beta": (placement.DataPlacementType.fine_all, myplacement.DP_1R, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.fine_all, myplacement.DP_1R, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.fine_all, myplacement.DP_1R, os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.fine_all, myplacement.DP_1R, os.path.join(script_dir, "placement", "powerlaw.json")),
        "uniform": (placement.DataPlacementType.fine_all, myplacement.DP_1R, os.path.join(script_dir, "placement", "uniform.json")),
    }
    if data_placement_type == 'coarse':
        data_placement_list = coarse_data_placement_list
    elif data_placement_type == 'fine':
        data_placement_list = fine_data_placement_list

    workload_distribution_list = {
        "beta": {"type": "beta", "alpha": 2, "beta": 5},
        "beta-high": {"type": "beta", "alpha": 2, "beta": 10},
        "beta-low": {"type": "beta", "alpha": 2, "beta": 2},
        "gamma": {"type": "gamma", "shape": 2},
        "gamma-high": {"type": "gamma", "shape": 1},
        "gamma-low": {"type": "gamma", "shape": 10},
        "normal": {"type": "normal"},
        "powerlaw": {"type": "powerlaw", "shape": 5},
        "powerlaw-high": {"type": "powerlaw", "shape": 10},
        "powerlaw-low": {"type": "powerlaw", "shape": 2},
        "uniform": {"type": "uniform"},
    }

    variable_setting_list = []
    for workload, data_placement in itertools.product(workload_distribution_list.items(), data_placement_list.items()):
        workload_name, workload_config = workload
        data_placement_name, data_placement_profile = data_placement
        # selective evaluations for shortening process
        #if not workload_name == 'powerlaw':
        #    continue
        #if not data_placement_name == 'beta':
        #    continue
        #if not data_placement_name == 'complete':
        #    continue
        #if 'fine' in data_placement_type:
        #    if not workload_name == data_placement_name:
        #        continue
        if data_placement_name not in workload_name:
            continue
        if selective_run:
            if data_placement_type == 'coarse':
                if not data_placement_name == "complete":
                    if not ((workload_name == data_placement_name) or data_placement_name == "uniform"):
                        continue
            elif 'fine' in data_placement_type:
                if not workload_name == data_placement_name:
                    continue
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, 'w-{}_dp-{}_{}'.format(workload_name, data_placement_name, run_id)),
            'experiment.data_placement': data_placement_profile,
            'experiment.dp_model': dp_model,
            'workload.distribution': workload_config,
        })

    for experiment in generate_experiments(default_setting, variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1, check_success=check_success):
        print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        experiment.run()
        #pass


if __name__ == "__main__":
    for run_id in range(1, 11):
        dp_set = [
            ('coarse', 'rainbow'),
            ('fine', 'rainbow'),
            ('fine', 'monochromatic'),
        ]
        for dp_type, dp_model in dp_set:
            # run_id, dp_type, dp_model, selective_run, check_success
            main(run_id, dp_type, dp_model, False, True)
