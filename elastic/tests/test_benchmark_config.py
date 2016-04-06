import unittest
import copy

import yaml

from elastic.benchmark.config import BaseConfig

class TestBenchmarkConfig(unittest.TestCase):

    def setUp(self):
        self.config_str = '''controller:
  commander: {port: 9999}
  host: 10.25.11.107
  job_queue: {port: 9998}
  manager: {port: 9996}
  num_drivers: 6
  report_queue: {port: 9997}
driver:
  hosts: [10.25.12.2, 10.25.11.110, 10.25.11.73, 10.25.11.71, 10.25.11.93, 10.25.13.44]
  num_workers: 128
hpcc:
  esp: {host: 152.46.16.135, port: 8002}
        '''
        self.config = BaseConfig(yaml.load(self.config_str))

    def tearDown(self):
        pass

    def test_get_key(self):
        self.assertEqual(self.config.get_config("controller"), self.config.config['controller'])

    def test_lookup_key(self):
        self.assertEqual(self.config.lookup_config("controller.num_drivers"), 6)
        self.assertEqual(self.config.lookup_config("controller.num_driverss", 7), 7)


if __name__ == "__main__":
    unittest.main()
