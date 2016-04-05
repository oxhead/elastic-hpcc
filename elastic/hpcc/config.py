import yaml
from lxml import etree


class HPCCConfig:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as f:
            return HPCCConfig(yaml.load(f))

    @staticmethod
    def parse_config(config_path):
        hpcc_config = {}
        hpcc_xml = None
        with open(config_path, 'r') as f:
            hpcc_xml = etree.parse(f)

        node_mapping = {}
        for node in hpcc_xml.xpath("//Environment/Hardware/Computer[@name]"):
            node_mapping[node.get('name')] = node.get('netAddress')

        roxie_cluster = []
        for node in hpcc_xml.xpath("//Environment/Software/RoxieCluster/RoxieServerProcess[@netAddress]"):
            roxie_cluster.append(node.get('netAddress'))

        thor_cluster = []
        for node in hpcc_xml.xpath("//Environment/Software/ThorCluster/ThorSlaveProcess[@computer]"):
            thor_cluster.append(node_mapping[node.get('computer')])

        hpcc_config = {
            "hosts": node_mapping,
            "roxie": roxie_cluster,
            "thor": thor_cluster
        }

        print(hpcc_config)





