import unittest
import copy

import yaml

from elastic.benchmark.workload import *


class TestBenchmarkWorkload(unittest.TestCase):

    def setUp(self):
        self.workload_str = '''workload:
    type: constant
    num_queries: 10
    period: 10
    distribution:
        type: pareto
        alpha: 3
    selection:
        type: uniform
    applications:
        anagram2:
            endpoint: http://152.46.16.135:8002/WsEcl/json/query/roxie/validateanagrams
            query_name: validateanagrams
            query_key: word
            key_file: /home/chsu6/elastic-hpcc/benchmark/dataset/word_list.txt
        originalperson:
            endpoint: http://152.46.16.135:8002/WsEcl/json/query/roxie/fetchpeoplebyzipservice
            query_name: fetchpeoplebyzipservice
            query_key: zipvalue
            key_file: /home/chsu6/elastic-hpcc/benchmark/dataset/zipcode_list.txt
        sixdegree:
            endpoint: http://152.46.16.135:8002/WsEcl/json/query/roxie/searchlinks
            query_name: searchlinks
            query_key: name
            key_file: /home/chsu6/elastic-hpcc/benchmark/dataset/actor_list.txt
        '''
        self.workload_config = WorkloadConfig(yaml.load(self.workload_str))
        self.workload = Workload.from_config(self.workload_config)

    def tearDown(self):
        pass

    def test_from_config(self):
        Workload.from_config(self.workload_config)

    def test_basic(self):
        w = Workload.from_config(self.workload_config)
        n = w.next()
        while n is not None:
            n = w.next()
        self.assertEqual(w.taken_count, w.period)

    def test_timeline(self):
        workload = Workload.from_config(self.workload_config)
        workload_timeline = WorkloadExecutionTimeline.from_workload(workload)
        self.assertEqual(workload.period, workload_timeline.period)

        count = 0
        for t, workload_items in workload_timeline.next():
            count += len(workload_items)
        self.assertEqual(count, 10*10)


if __name__ == "__main__":
    unittest.main()
