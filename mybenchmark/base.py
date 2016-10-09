import os
import copy
import pprint
import pickle
import json
import hashlib

from elastic import init
from elastic.benchmark.config import BaseConfig
from elastic.benchmark.roxie import *
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.workload import Workload, WorkloadConfig, WorkloadExecutionTimeline
from elastic.hpcc import placement
from elastic.hpcc import roxie
from elastic.hpcc.service import HPCCService

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

    def cache(self, workload_config, workload, update=False):
        cache_path = self._generate_cache_path(workload_config)
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
    def __init__(self, experiment_id, benchmark_config, hpcc_cluster, workload_timeline, output_dir, dp=None, wait_time=60, check_success=True):
        self.experiment_id = experiment_id
        self.benchmark_config = benchmark_config
        self.hpcc_cluster = hpcc_cluster
        self.hpcc_service = HPCCService(self.hpcc_cluster)
        self.workload_timeline = workload_timeline
        self.output_dir = output_dir
        self.dp = dp
        self.wait_time = wait_time
        self.check_success = check_success

    def pre_run(self):
        self.hpcc_service.stop()
        self.hpcc_service.clear_log()  # to get the right counter
        self.hpcc_service.clean_system()  # to make the same base
        if self.dp is not None:
            roxie.switch_data_placement(self.dp)
        self.hpcc_service.start()

    def post_run(self):
        pass

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
            return
        try:
            self.pre_run()
            bm = RoxieBenchmark(self.hpcc_cluster, self.benchmark_config, self.workload_timeline, output_dir=self.output_dir)
            time.sleep(self.wait_time)
            bm.run()
            self.post_run()
            if self.check_success and not self.check_successful():
                print("The experiment did not succeed and requires to rerun")
                os.system("rm -rf {}".format(self.output_dir))
                time.sleep(60)  # wait 60 seconds for recovering??
                self.run()
        except Exception as e:
            print("Failed to run the experiment", e)


def generate_experiments(default_setting, variable_setting_list, experiment_dir=None, timeline_reuse=False, wait_time=60, check_success=True):
    for variable_setting in variable_setting_list:
        per_setting = copy.deepcopy(default_setting)

        for setting_name, setting_value in variable_setting.items():
            per_setting.set_config(setting_name, setting_value)

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
        workload_timeline = workload_timeline_manager.cache(workload_config, workload, update=not timeline_reuse)
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
        num_benchmark_clients = int(per_setting['experiment.benchmark_clients'])
        num_benchmark_concurrency = int(per_setting['experiment.benchmark_concurrency'])
        benchmark_config.set_config("driver.hosts", benchmark_config.lookup_config("driver.hosts")[:num_benchmark_clients])
        benchmark_config.set_config("driver.num_workers", num_benchmark_concurrency)
        #print("after", benchmark_config.config)

        if not per_setting.has_key('experiment.output_dir'):
            per_setting.set_config('experiment.output_dir', generate_default_output_dir(per_setting, hpcc_cluster, workload_config))
        output_dir = per_setting['experiment.output_dir']

        dp_new = None
        if per_setting.has_key('experiment.data_placement'):
            data_placement_type, old_locations, access_profile = per_setting['experiment.data_placement']
            dp_old = placement.DataPlacement.new(old_locations)
            #print("@@", per_setting['experiment.output_dir'])
            #print('@@ access_profile:', access_profile)
            access_statistics = placement.PlacementTool.load_statistics(access_profile)
            new_nodes = [n.get_ip() for n in hpcc_cluster.get_roxie_cluster().nodes]
            node_statistics = placement.PlacementTool.compute_node_statistics(access_statistics)
            print("Host statistics")
            print(json.dumps(node_statistics, indent=4, sort_keys=True))
            if data_placement_type == placement.DataPlacementType.coarse_partial:
                dp_new = placement.CoarseGrainedDataPlacement.compute_optimal_placement(dp_old, new_nodes, access_statistics)
                print("Data placement")
                print(json.dumps(dp_new.locations, indent=4, sort_keys=True))
            elif data_placement_type == placement.DataPlacementType.fine_partial:
                dp_new = placement.FineGrainedDataPlacement.compute_optimal_placement(dp_old, new_nodes, access_statistics)
                print("Data placement")
                print(json.dumps(dp_new.locations, indent=4, sort_keys=True))

        experiment = Experiment(experiment_id, benchmark_config, hpcc_cluster, workload_timeline, output_dir, dp=dp_new, wait_time=wait_time, check_success=check_success)
        experiment.workload_config = workload_config  # hack
        yield experiment


def analyze_timeline(timeline):
    from collections import defaultdict
    ds = defaultdict(lambda: 0)
    for k, vs in timeline.items():
        for v in vs:
            ds[v.query_name] += 1
    print(json.dumps(ds, indent=4))