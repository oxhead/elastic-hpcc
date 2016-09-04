import json

from elastic.hpcc import placement


dp_file = "/home/chsu6/elastic-hpcc/results/test/80rthreads_4bmclients_32bmthreads_full8_8N_xR_100q_30sec/uniform_1/result/access_distribution.json"
new_nodes = ['x1', 'x2']

old_dp = placement.DataPlacement.from_file(dp_file)
new_dp = placement.CoarseGrainedDataPlacement.compute_optimal_placement(old_dp, new_nodes, json.load(open(dp_file, 'r')))
#new_dp = placement.FineGrainedDataPlacement.compute_optimal_placement(old_dp, new_nodes, json.load(open(dp_file, 'r')))