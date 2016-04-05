from enum import Enum

from lxml import etree

from elastic import base


class ClusterType(Enum):
    esp = 0
    thor = 1
    roxie = 2


class Cluster(base.Cluster):
    def __init__(self, cluster_type, nodes):
        self.cluster_type = cluster_type
        self.nodes = nodes


class HPCCCluster(base.Cluster):

    def __init__(self, esp_cluster, thor_cluster, roxie_cluster):
        self.cluster_records = {
            ClusterType.esp: esp_cluster,
            ClusterType.thor: thor_cluster,
            ClusterType.roxie: roxie_cluster
        }

    def get_nodes(self):
        s = set()
        s.update(self.get_esp_cluster().get_nodes())
        s.update(self.get_thor_cluster().get_nodes())
        s.update(self.get_roxie_cluster().get_nodes())
        return s
        # return sorted(list(s), key=lambda node: node.hostname)

    def get_esp_cluster(self):
        return self.cluster_records[ClusterType.esp]

    def get_roxie_cluster(self):
        return self.cluster_records[ClusterType.roxie]

    def get_thor_cluster(self):
        return self.cluster_records[ClusterType.thor]

    @staticmethod
    def parse_config(config_path):
        with open(config_path, 'r') as f:
            hpcc_xml = etree.parse(f)

        node_mapping = {}
        for node in hpcc_xml.xpath("//Environment/Hardware/Computer[@name]"):
            node_mapping[node.get('name')] = node.get('netAddress')

        esp_nodes = []
        for node in hpcc_xml.xpath("//Environment/Software/EspProcess[@controlPort='8010']"):
            instance = node.find('Instance')
            esp_nodes.append(base.Node(instance.get('computer'), instance.get('netAddress')))

        roxie_nodes = []
        for node in hpcc_xml.xpath("//Environment/Software/RoxieCluster/RoxieServerProcess[@netAddress]"):
            roxie_nodes.append(base.Node(node.get('name'), node.get('netAddress')))

        thor_nodes = []
        for node in hpcc_xml.xpath("//Environment/Software/ThorCluster/ThorSlaveProcess[@computer]"):
            thor_nodes.append(base.Node(node.get('computer'), node_mapping[node.get('computer')]))

        cluster = HPCCCluster(
            Cluster(ClusterType.esp, esp_nodes),
            Cluster(ClusterType.thor, thor_nodes),
            Cluster(ClusterType.roxie,  roxie_nodes),
        )

        return cluster

