import itertools
import copy

from elastic.hpcc import placement

from mybenchmark.base import config as myconfig
from mybenchmark.base import *
from mybenchmark.E24 import myplacement

def main(dp_type, dp_model, selective_run, check_success, num_iters=1):
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    #####################################
    # experiment setting
    #####################################
    # 1. workload parameters
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target', "/home/chsu6/elastic-hpcc/template/elastic_1thor_8roxie_locality_nfs.xml")
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E27')
    default_setting.set_config('experiment.goal', '''
    1. Test manual request-dispatch routing
    ''')
    default_setting.set_config('experiment.conclusion', '''
        1. Seems 4 driver nodes and 16 threads per node is enough to drive the maximum throughput
        ''')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template.yaml"))
    default_setting.set_config('experiment.workload_endpoints', 8)
    # total driver clients = benchmark_processors * benchmark_clients
    default_setting.set_config('experiment.benchmark_processors', 2)
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.benchmark_manual_routing_table', True)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    default_setting.set_config('experiment.data_dir', '/dataset')
    default_setting.set_config('experiment.storage_type', 'nfs')
    # this only uses one query application
    #default_setting.set_config('experiment.applications', ['sequential_search_firstname', 'sequential_search_lastname', 'sequential_search_city', 'sequential_search_zip'])
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 150)
    default_setting.set_config('workload.dispatch_mode', 'batch')
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
    node_list = ['10.25.2.131', '10.25.2.132', '10.25.2.133', '10.25.2.134']
    partition_locations = generate_partition_locations(node_list, 1024, '/dataset/roxie/mybenchmark/data_sorted_people_firstname_{}._1_of_1')
    #import json
    #print(json.dumps(partition_locations, indent=4))
    #import sys
    #sys.exit(0)
    coarse_data_placement_list = {
        #"beta": (placement.DataPlacementType.coarse_partial, partition_locations, os.path.join(script_dir, "placement", "beta.json")),
        #"gamma": (placement.DataPlacementType.coarse_partial, partition_locations, os.path.join(script_dir, "placement", "gamma.json")),
        #"normal": (placement.DataPlacementType.coarse_partial, partition_locations, os.path.join(script_dir, "placement", "normal.json")),
        #"powerlaw": (placement.DataPlacementType.coarse_partial, partition_locations, os.path.join(script_dir, "placement", "powerlaw.json")),
        #"uniform": (placement.DataPlacementType.coarse_partial, partition_locations, os.path.join(script_dir, "placement", "uniform.json")),
        "complete": (placement.DataPlacementType.complete, partition_locations, os.path.join(script_dir, "placement", "uniform.json")),
    }
    fine_data_placement_list = {
        "beta": (placement.DataPlacementType.fine_all, partition_locations, os.path.join(script_dir, "placement", "beta.json")),
        "gamma": (placement.DataPlacementType.fine_all, partition_locations, os.path.join(script_dir, "placement", "gamma.json")),
        "normal": (placement.DataPlacementType.fine_all, partition_locations, os.path.join(script_dir, "placement", "normal.json")),
        "powerlaw": (placement.DataPlacementType.fine_all, partition_locations, os.path.join(script_dir, "placement", "powerlaw.json")),
        "uniform": (placement.DataPlacementType.fine_all, partition_locations, os.path.join(script_dir, "placement", "uniform.json")),
    }
    if data_placement_type == 'coarse':
        data_placement_list = coarse_data_placement_list
    elif data_placement_type == 'fine':
        data_placement_list = fine_data_placement_list

    workload_distribution_list = {
        "uniform-base": {"type": "uniform"},
        #"beta-least": {"type": "beta", "alpha": 1, "beta": 1},
        #"beta-less": {"type": "beta", "alpha": 1.5, "beta": 1.5},
        "beta-base": {"type": "beta", "alpha": 2, "beta": 2},
        #"beta-more": {"type": "beta", "alpha": 4, "beta": 4},
        #"beta-most": {"type": "beta", "alpha": 5, "beta": 5},
        "normal-base": {"type": "normal", "loc": 0, "scale": 1},
        #"powerlaw-least": {"type": "powerlaw", "shape": 2},
        #"powerlaw-less": {"type": "powerlaw", "shape": 2.5},
        "powerlaw-base": {"type": "powerlaw", "shape": 3},
        #"powerlaw-more": {"type": "powerlaw", "shape": 4},
        #"powerlaw-most": {"type": "powerlaw", "shape": 5},
        #"gamma-least": {"type": "gamma", "shape": 7},
        #"gamma-less": {"type": "gamma", "shape": 6},
        "gamma-base": {"type": "gamma", "shape": 5},
        #"gamma-more": {"type": "gamma", "shape": 4},
        #"gamma-most": {"type": "gamma", "shape": 3},
    }

    variable_setting_list = []
    for workload, data_placement in itertools.product(workload_distribution_list.items(), data_placement_list.items()):
        workload_name, workload_config = workload
        data_placement_name, data_placement_profile = data_placement
        # selective evaluations for shortening process
        if selective_run:
            if data_placement_type == 'coarse':
                if data_placement_name == 'uniform' or data_placement_name == 'complete':
                    pass
                elif data_placement_name in workload_name:
                    pass
                else:
                    continue
            elif 'fine' in data_placement_type:
                if not (data_placement_name in workload_name):
                    continue
        #if workload_config['type'] != data_placement_name:
        #    continue
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, 'w-{}_dp-{}'.format(workload_name, data_placement_name)),
            'experiment.data_placement': data_placement_profile,
            'experiment.dp_name': "dp_{}_{}_{}".format(dp_type, dp_model, data_placement_name) if data_placement_name != 'complete' else 'dp_complete',
            'experiment.dp_model': dp_model,
            'workload.selection': workload_config, # different from previous experiment
            'workload.name': workload_name,
        })

    # required to reduce time for switching data placement
    variable_setting_list = list(sorted(variable_setting_list, key=lambda x: x['experiment.dp_name']))

    final_variable_setting_list = []
    for variable_setting in variable_setting_list:
        for run_id in range(1, num_iters+1):
            variable_setting_copied = copy.copy(variable_setting)
            variable_setting_copied['experiment.output_dir'] = "{}_{}".format(variable_setting_copied['experiment.output_dir'], run_id)
            # print(variable_setting_copied['experiment.output_dir'])
            final_variable_setting_list.append(variable_setting_copied)
            # print(variable_setting_copied['experiment.dp_name'])

    for experiment in generate_experiments(default_setting, final_variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1, check_success=check_success, restart_hpcc=True, timeout=500):
        print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        experiment.run()
        #pass


if __name__ == "__main__":
    dp_set = [
        ('coarse', 'rainbow'),
        #('fine', 'rainbow'),
        #('fine', 'monochromatic')
    ]
    for dp_type, dp_model in dp_set:
        # dp_type, dp_model, selective_run, check_success
        main(dp_type, dp_model, True, True, num_iters=3)
