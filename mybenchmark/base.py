import os
import copy
import pprint
import pickle
import json
import hashlib
import shutil
import itertools

from elastic import init
from elastic.benchmark.config import BaseConfig
from elastic.benchmark.roxie import *
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.workload import Workload, WorkloadConfig, WorkloadExecutionTimeline
from elastic.hpcc import placement
from elastic.hpcc import roxie
from elastic.hpcc.service import HPCCService
from elastic.simulator import dp_simulation

from elastic.hpcc.base import *
from elastic.util import helper


def generate_default_output_dir(setting, hpcc_cluster, workload):
    run_output_dir = os.path.join(
        setting['experiment.result_dir'],
        setting['experiment.id'],
        "{}roxie_{}_{}_{}queries_{}sec".format(
            hpcc_cluster.get_roxie_cluster().get_num_nodes(),
            workload['workload.type'],
            workload['workload.distribution.type'],
            workload['workload.num_queries'],
            workload['workload.period']
        )
     )
    return run_output_dir


class WorkloadTimelineManager:
    @staticmethod
    def new():
        return WorkloadTimelineManager()

    @staticmethod
    def get_workload_timeline(workload):
        return WorkloadTimelineManager.new().cache(workload)

    @staticmethod
    def save_timeline(workload_timeline, cache_path):
        workload_timeline.to_pickle(cache_path)

    @staticmethod
    def load_timeline(cache_path):
        return WorkloadExecutionTimeline.from_pickle(cache_path)

    def __init__(self, store_dir=".workload_timeline"):
        self.store_dir = store_dir
        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir)

    def _generate_cache_path(self, workload_config):
        #print('----------------')
        #print(repr(workload))
        md5_key = hashlib.md5(json.dumps(workload_config.config, sort_keys=True).encode()).hexdigest()
        return os.path.join(self.store_dir, md5_key)

    def cache(self, workload_config, workload, update=False, name=None):
        cache_path = self._generate_cache_path(workload_config) if name is None else os.path.join(self.store_dir, name)
        if (not update) and (os.path.exists(cache_path)):
            print("loading workload timeline from {}".format(cache_path))
            return WorkloadTimelineManager.load_timeline(cache_path)
        else:
            print("generate a new workload timeline")
            workload_timeline = WorkloadExecutionTimeline.from_workload(workload)
            print("caching the workload timeline to {}".format(cache_path))
            WorkloadTimelineManager.save_timeline(workload_timeline, cache_path)
            return workload_timeline


class ExperimentConfig(BaseConfig):
    @staticmethod
    def new():
        default_config = ExperimentConfig({})
        default_result_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "results")
        default_config.set_config('experiment.result_dir', default_result_dir)
        return default_config


class Experiment:
    def __init__(self, experiment_id, benchmark_config, hpcc_cluster, workload_timeline, output_dir, wp=None, dp=None, wait_time=60, check_success=True, data_dir='/dataset', storage_type='nfs', restart_hpcc=False, routing_table={}):
        self.experiment_id = experiment_id
        self.benchmark_config = benchmark_config
        self.routing_table = routing_table
        self.hpcc_cluster = hpcc_cluster
        self.hpcc_service = HPCCService(self.hpcc_cluster)
        self.workload_timeline = workload_timeline
        self.output_dir = output_dir
        self.wp = wp
        self.dp = dp
        self.wait_time = wait_time
        self.check_success = check_success
        self.data_dir = data_dir
        self.storage_type = storage_type
        self.restart_hpcc = restart_hpcc

    def pre_run(self):
        if self.restart_hpcc:
            self.hpcc_service.stop()
            self.hpcc_service.clear_log()  # to get the right counter
        else:
            self.hpcc_service.truncate_log()  # remove all logs
        self.hpcc_service.clean_system()  # to make the same base
        if self.dp is not None and self.dp.name is not None:
            dp_name_file = '/tmp/dp_name'
            previous_dp_name = ""
            if os.path.exists(dp_name_file):
                with open(dp_name_file, 'r') as f:
                    previous_dp_name = f.read().strip()
            if previous_dp_name == "" or self.dp.name != previous_dp_name:
                print("Switch data placement from {} to {}".format(previous_dp_name, self.dp.name))
                with open(dp_name_file, 'w') as f:
                    print('Writing dp_name {} to {}'.format(self.dp.name, dp_name_file))
                    f.write(self.dp.name)
                roxie.switch_data_placement(self.dp, data_dir=self.data_dir, storage_type=self.storage_type)
            else:
                print("No need to switch data placement")
        if self.restart_hpcc:
            self.hpcc_service.start()

    def post_run(self):
        wp_path = os.path.join(self.output_dir, "result", "workload_profile.json")
        if self.wp is not None:
            shutil.copyfile(self.wp, wp_path)
        dp_path = os.path.join(self.output_dir, "result", "data_placement.json")
        if self.dp is not None:
            with open(dp_path, 'w') as f:
                json.dump(self.dp.locations, f, indent=4, sort_keys=True)

    def check_successful(self):
        # hard coded here, or should do the check in RoxieBenchmark?
        report_path = os.path.join(self.output_dir, "result", "report.json")
        if not os.path.exists(report_path):
            return False
        else:
            try:
                with open(report_path, 'r') as f:
                    report_json = json.load(f)
                    if int(report_json['num_failed_jobs']) != 0:
                        print("# of failed queries:", int(report_json['num_failed_jobs']))
                        return False
            except:
                return False
        return True

    def run(self):
        print("Experiment:", self.experiment_id)
        print("Nodes:", self.hpcc_cluster.get_roxie_cluster().get_num_nodes())
        print("Output:", self.output_dir)
        if os.path.exists(self.output_dir):
            print("The output directory exists")
            return False
        try:
            self.pre_run()
            bm = RoxieBenchmark(self.hpcc_cluster, self.benchmark_config, self.workload_timeline, output_dir=self.output_dir, routing_table=self.routing_table)
            time.sleep(self.wait_time)
            bm.run()
            if self.check_success and not self.check_successful():
                print("The experiment did not succeed and requires to rerun")
                os.system("rm -rf {}".format(self.output_dir))
                time.sleep(60)  # wait 60 seconds for recovering??
                self.run()
            self.post_run()
            return True
        except Exception as e:
            print("Failed to run the experiment", e)
            import traceback
            traceback.print_exc()
        return False


def generate_experiments(default_setting, variable_setting_list, experiment_dir=None, timeline_reuse=False, wait_time=60, check_success=True, overwrite=False, restart_hpcc=False):
    for variable_setting in variable_setting_list:
        per_setting = copy.deepcopy(default_setting)
        #print(json.dumps(per_setting.config, indent=4))
        for setting_name, setting_value in variable_setting.items():
            per_setting.set_config(setting_name, setting_value)
        #print(json.dumps(per_setting.config, indent=4))
        # create workload timeline
        workload_config = WorkloadConfig.parse_file(per_setting['experiment.workload_template'])
        # this feature now can reduce redundancy workload configs
        if per_setting.has_key('experiment.workload_endpoints'):
            workload_config.select_endpoints(per_setting.lookup_config('experiment.workload_endpoints'))
        workload_config.merge(per_setting)  # should be able to merge

        if per_setting.has_key('experiment.applications'):
            application_db = workload_config.lookup_config('workload.applications')
            app_names = per_setting['experiment.applications']
            app_config = {}
            for app_name in app_names:
                app_config[app_name] = application_db[app_name]
            workload_config.set_config('workload.applications', app_config)

        print(json.dumps(workload_config.config, indent=4))

        workload = Workload.from_config(workload_config)
        workload_timeline_dir = os.path.join(experiment_dir, '.workload_timeline') if experiment_dir else '.workload_timeline'
        workload_timeline_manager = WorkloadTimelineManager(store_dir=workload_timeline_dir)
        workload_name = per_setting['workload.name'] if per_setting.has_key('workload.name') else None
        workload_timeline = workload_timeline_manager.cache(workload_config, workload, update=not timeline_reuse, name=workload_name)
        #for k, vs in workload_timeline.timeline.items():
        #    for v in vs:
        #        print(v.wid, v.query_name, v.key)
        #print(workload.application_selection.distribution, workload.application_selection.probability_list)
        #analyze_timeline(workload_timeline.timeline)
        #import sys
        #sys.exit(0)

        experiment_id = per_setting['experiment.id']
        hpcc_cluster = HPCCCluster.parse_config(per_setting['cluster.target'])
        benchmark_config = BenchmarkConfig.parse_file(per_setting['cluster.benchmark'])
        #print("before", benchmark_config.config)
        num_benchmark_processors_per_client = int(per_setting['experiment.benchmark_processors'])
        num_benchmark_clients = int(per_setting['experiment.benchmark_clients'])
        num_benchmark_concurrency = int(per_setting['experiment.benchmark_concurrency'])
        benchmark_config.set_config("driver.num_processors", num_benchmark_processors_per_client)
        benchmark_config.set_config("driver.hosts", benchmark_config.lookup_config("driver.hosts")[:num_benchmark_clients])
        benchmark_config.set_config("driver.num_workers", num_benchmark_concurrency)
        #print("after", benchmark_config.config)

        if not per_setting.has_key('experiment.output_dir'):
            per_setting.set_config('experiment.output_dir', generate_default_output_dir(per_setting, hpcc_cluster, workload_config))
        output_dir = per_setting['experiment.output_dir']

        if (not overwrite) and os.path.exists(output_dir):
            print("skip experiment:", output_dir)
            continue

        dp_new = None
        routing_table = {}
        access_profile = None
        if per_setting.has_key('experiment.data_placement'):
            data_placement_type, old_locations, access_profile = per_setting['experiment.data_placement']
            dp_model = per_setting['experiment.dp_model']
            dp_old = placement.DataPlacement.new(old_locations)
            #new_nodes = [n.get_ip() for n in hpcc_cluster.get_roxie_cluster().nodes]
            old_nodes = sorted(dp_old.nodes)
            new_nodes = sorted(list(set([n.get_ip() for n in hpcc_cluster.get_roxie_cluster().nodes]) - set(dp_old.nodes)))

            access_statistics = placement.PlacementTool.compute_partition_statistics(placement.PlacementTool.load_statistics(access_profile))
            coarse_grained = True if data_placement_type == placement.DataPlacementType.coarse_partial else False
            dp_name = per_setting['experiment.dp_name']
            if data_placement_type == placement.DataPlacementType.complete:
                all_nodes = old_nodes + new_nodes
                dp_new = generate_complete_data_placement(all_nodes, old_locations)
            else:
                dp_new = generate_data_placement(old_nodes, new_nodes, old_locations, access_statistics, coarse_grained=coarse_grained, dp_model=dp_model, dp_name=dp_name)
            #print(json.dumps(dp_new.locations, indent=4, sort_keys=True))
            #import sys
            #sys.exit(0)
            if per_setting.has_key('experiment.benchmark_manual_routing_table'):
                routing_table = generate_routing_table(dp_new, workload_config.lookup_config('workload.endpoints'))

        data_dir = per_setting['experiment.dataset_dir'] if per_setting.has_key('experiment.dataset_dir') else '/dataset'
        storage_type = per_setting['experiment.storage_type'] if per_setting.has_key('experiment.storage_type') else 'nfs'
        experiment = Experiment(experiment_id, benchmark_config, hpcc_cluster, workload_timeline, output_dir, wp=access_profile, dp=dp_new, wait_time=wait_time, check_success=check_success, data_dir=data_dir, storage_type=storage_type, restart_hpcc=restart_hpcc, routing_table=routing_table)
        experiment.workload_config = workload_config  # hack
        yield experiment

def roxie_node_comparator(node_ip):
    try:
        return int(node_ip.split('.')[-1])
    except:
        pass
    return node_ip

def roxie_file_comparator(file_name):
    try:
        #print(file_name)
        #print((int(file_name.split('.')[0].split('_')[-1]), int(file_name.split('.')[-1].split('_')[1])))
        return (int(file_name.split('.')[0].split('_')[-1]), int(file_name.split('.')[-1].split('_')[1]))
    except:
        pass
    return file_name


def generate_data_placement(old_nodes, new_nodes, locations, access_statistics, coarse_grained=True, dp_model='rainbow', dp_name=None):
    # assign node id
    nodes = sorted(set(old_nodes + new_nodes))  # should not have duplicate nodes
    #print("nodes:", nodes)
    # assign partition id -> k=1 or k > 1
    max_num_partitions = 1
    if not coarse_grained:
        max_num_partitions = 1
        for node in old_nodes:
            if len(locations[node]) > max_num_partitions:
                max_num_partitions = len(locations[node])
    #print("k:", max_num_partitions)
    #print(json.dumps(locations, indent=4))
    #print(json.dumps(access_statistics, indent=4))
    # create access frequency list
    partition_list = []
    af_list = []
    sorted_partition_list = []
    sorted_af_list = []
    if max_num_partitions == 1:
        for node in old_nodes:
            #print(node, sum([(access_statistics[partition] if partition in access_statistics else 0) for partition in locations[node]]))
            af_list.append(sum([(access_statistics[partition] if partition in access_statistics else 0) for partition in locations[node]]))
            partition_list.append(node)
        # sort by node ip for now
        for node in sorted(partition_list, key=roxie_node_comparator):
            sorted_partition_list.append(node)
            sorted_af_list.append(af_list[partition_list.index(node)])
    else:
        for partition in sorted([partition for node in locations.keys() for partition in locations[node]]):
            af_list.append(access_statistics[partition] if partition in access_statistics else 0)
            partition_list.append(partition)
        # this can be an issue
        for partition in sorted(partition_list, key=roxie_file_comparator):
            sorted_partition_list.append(partition)
            sorted_af_list.append(af_list[partition_list.index(partition)])
    #print('-----------')
    for i in range(len(sorted_partition_list)):
        print(sorted_partition_list[i], sorted_af_list[i])
    M = len(old_nodes)
    N = len(nodes)
    k = max_num_partitions
    t = dp_model
    print('+++Running data placement simulation+++')
    dp_records, adjusted_num_replicas_list = dp_simulation.run(M, N, k, t, af_list=sorted_af_list)

    # print(json.dumps(dp_records, indent=4))

    new_locations = {}
    if max_num_partitions == 1:
        #print('................')
        #print(json.dumps(dp_records, indent=4))
        for node_name in dp_records.keys():
            new_locations[nodes[int(node_name)]] = []
            for node_index in dp_records[node_name]:
                new_locations[nodes[int(node_name)]].extend(locations[nodes[node_index]])
    else:
        for node_name in dp_records.keys():
            new_locations[nodes[int(node_name)]] = []
            for partition_index in dp_records[node_name]:
                new_locations[nodes[int(node_name)]].append(sorted_partition_list[partition_index])
    return placement.DataPlacement(nodes, sorted_partition_list, new_locations, name=dp_name)


def generate_complete_data_placement(node_list, partition_locations):
    sorted_partition_list = list(sorted(set([x for sublist in partition_locations.values() for x in sublist]), key=roxie_file_comparator))
    locations = {node: sorted_partition_list for node in node_list}
    return placement.DataPlacement(node_list, locations[node_list[0]], locations, name='dp_complete')


def generate_partition_locations(node_list, num_partitions, partition_name_template):
    partition_locations = {}
    for node in node_list:
        partition_locations[node] = []
    num_partitions_per_node = int(num_partitions / len(node_list))
    for i in range(len(node_list)):
        node = node_list[i]
        for j in range(1, num_partitions_per_node+1):
            partition_id = num_partitions_per_node * i + j
            partition_locations[node].append(partition_name_template.format(partition_id))
    return partition_locations

def generate_routing_table(dp, endpoints, query_name='sequential_search_firstname_'):
    endpoint_table = {}
    routing_table = {}

    # dp should use the host/ip as the key?
    for endpoint in endpoints:
        host = endpoint.split('/')[-1].split(':')[0]
        endpoint_table[host] = endpoint

    for node, partitions in dp.locations.items():
        for partition in partitions:
            app_id = partition.split('/')[-1].split('.')[0].split('_')[-1]  # need to extract app id from the partition
            mapped_query_name = query_name + app_id
            if mapped_query_name not in routing_table:
                routing_table[mapped_query_name] = []
            # the order should not matter?
            routing_table[mapped_query_name].append(endpoint_table[node])
    #print(json.dumps(routing_table, indent=4))
    #print('size={}'.format(len(routing_table)))
    #import sys
    #sys.exit(0)
    return routing_table

def analyze_timeline(timeline):
    distribution_records = {}
    num_partitions = 1024
    num_hosts = 4
    for i in range(1, num_partitions + 1):
        distribution_records[i] = 0

    for t, items in timeline.items():
        for item in items:
            app_id = int(item.query_name.split('_')[-1])
            distribution_records[app_id] += 1
            # print(t, item.wid, item.endpoint, item.query_name, item.query_key, item.key)

    for i in range(1, num_partitions + 1):
        print('P{}'.format(i), distribution_records[i])

    num_partitions_per_host = int(num_partitions / num_hosts)
    for i in range(num_hosts):
        count = 0
        for j in range(1, num_partitions_per_host + 1):
            count += distribution_records[i * num_partitions_per_host + j]
        print('host_{}:'.format(i+1), count)