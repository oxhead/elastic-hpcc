import sys
import os
import json
import importlib

from elastic.hpcc.base import *
from elastic.hpcc import placement

# http://stackoverflow.com/questions/547829/how-to-dynamically-load-a-python-class
def my_import(name):
    print(name)
    components = name.split('.')
    mod = __import__(components[0])
    for comp in components[1:]:
        print(help(mod))
        mod = getattr(mod, comp)
    return mod


def show_layout(experiment_id, dp_type_name, dp_name):

    hpcc_config = '/home/chsu6/elastic-hpcc/template/elastic_4thor_8roxie_locality.xml'
    hpcc_cluster = HPCCCluster.parse_config(hpcc_config)
    script_dir = os.path.dirname(os.path.realpath(__file__))

    myplacement = importlib.import_module("mybenchmark.{}.myplacement".format(experiment_id))
    #dp_type = placement.DataPlacementType.coarse_partial if dp_type_name == "coarse" else placement.DataPlacementType.fine_partial
    dp_type = placement.DataPlacementType[dp_type_name]
    access_profile = os.path.join(script_dir, "..", "mybenchmark", experiment_id, "placement", "{}.json".format(dp_name))

    print(experiment_id, dp_type, access_profile)
    dp_old = placement.DataPlacement.new(myplacement.DP_1R)
    access_statistics = placement.PlacementTool.load_statistics(access_profile)
    new_nodes = [n.get_ip() for n in hpcc_cluster.get_roxie_cluster().nodes]
    node_statistics = placement.PlacementTool.compute_node_statistics(access_statistics)
    print("Host statistics")
    print(json.dumps(node_statistics, indent=4, sort_keys=True))
    if dp_type == placement.DataPlacementType.coarse_partial:
        dp_new = placement.CoarseGrainedDataPlacement.compute_optimal_placement(dp_old, new_nodes, access_statistics)
        print("Data placement")
        print(json.dumps(dp_new.locations, indent=4, sort_keys=True))
    elif dp_type == placement.DataPlacementType.fine_partial:
        dp_new = placement.FineGrainedDataPlacement.compute_optimal_placement(dp_old, new_nodes, access_statistics)
        print("Data placement")
        print(json.dumps(dp_new.locations, indent=4, sort_keys=True))
    elif dp_type == placement.DataPlacementType.fine_all:
        dp_new = placement.FineGrainedDataPlacement.compute_optimal_placement_complete(dp_old, new_nodes, access_statistics)
        print("Data placement")
        print(json.dumps(dp_new.locations, indent=4, sort_keys=True))


if __name__ == "__main__":
    experiment_id, dp_type, dp_name = sys.argv[1:]
    show_layout(experiment_id, dp_type, dp_name)
