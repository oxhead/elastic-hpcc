import itertools

from elastic.hpcc import placement

from mybenchmark.base import config as myconfig
from mybenchmark.base import *


def main(run_id):
    init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # cluster setting
    default_setting = ExperimentConfig.new()
    default_setting.set_config('cluster.target',
                               "/home/chsu6/elastic-hpcc/template/elastic_1thor_8roxie_locality_nfs.xml")
    default_setting.set_config('cluster.deploy_config', False)
    default_setting.set_config('cluster.benchmark', "/home/chsu6/elastic-hpcc/conf/benchmark_template.yaml")
    default_setting.set_config('experiment.id', 'E40')
    default_setting.set_config('experiment.goal', '''
        1. Test manual request-dispatch routing
        ''')
    default_setting.set_config('experiment.conclusion', '''
            1. Seems 4 driver nodes and 16 threads per node is enough to drive the maximum throughput
            ''')
    default_setting.set_config('experiment.workload_template', os.path.join(script_dir, "workload_template.yaml"))
    default_setting.set_config('experiment.workload_endpoints', 8)
    # total driver clients = benchmark_processors * benchmark_clients
    default_setting.set_config('experiment.benchmark_instances', 8)
    default_setting.set_config('experiment.benchmark_processors', 2)
    default_setting.set_config('experiment.benchmark_clients', 4)
    default_setting.set_config('experiment.benchmark_concurrency', 32)
    default_setting.set_config('experiment.benchmark_manual_routing_table', True)
    default_setting.set_config('experiment.roxie_concurrency', 80)
    default_setting.set_config('experiment.dataset_dir', '/dataset')
    default_setting.set_config('experiment.storage_type', 'nfs')
    # this only uses one query application
    # default_setting.set_config('experiment.applications', ['sequential_search_firstname', 'sequential_search_lastname', 'sequential_search_city', 'sequential_search_zip'])
    default_setting.set_config('workload.num_queries', 200)
    default_setting.set_config('workload.period', 150)
    default_setting.set_config('workload.dispatch_mode', 'batch')
    default_setting.set_config('workload.type', 'constant')
    # default_setting.set_config('workload.distribution', {"type": "powerlaw", "shape": 5})
    default_setting.set_config('workload.selection', {"type": "powerlaw", "shape": 5})

    output_dir = os.path.join(
        default_setting['experiment.result_dir'],
        default_setting['experiment.id'],
        "placement"
    )

    node_list = ['10.25.2.131', '10.25.2.132', '10.25.2.133', '10.25.2.134']
    partition_locations = generate_partition_locations(node_list, 128, '/dataset/roxie/mybenchmark/data_sorted_people_firstname_{}._1_of_1')
    data_placement_profile = (placement.DataPlacementType.complete, partition_locations, None)

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
    for workload_name, workload_config in workload_distribution_list.items():
        variable_setting_list.append({
            'experiment.output_dir': os.path.join(output_dir, workload_name),
            'experiment.data_placement': data_placement_profile,
            'experiment.dp_name': "dp_complete",
            'experiment.dp_model': "",
            'workload.selection': workload_config, # different from previous experiment
            'workload.name': workload_name,
        })

    os.system("mkdir -p {}/placement".format(script_dir))
    for experiment in generate_experiments(default_setting, variable_setting_list, experiment_dir=script_dir, timeline_reuse=True, wait_time=1, check_success=True, restart_hpcc=True, timeout=500):
        #print(experiment.output_dir)
        #helper.json_pretty_print(experiment.dp.locations)
        if experiment.run():
            os.system("cp {}/result/access_distribution.json {}/placement/{}.json".format(experiment.output_dir, script_dir, experiment.workload_config.lookup_config('workload.selection.type')))
        #break

if __name__ == "__main__":
    # repeat how many times
    for run_id in range(1, 2):
        main(run_id)
