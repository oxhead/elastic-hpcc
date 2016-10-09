import os
import collections
import logging

from executor import execute
from executor.ssh.client import RemoteCommand
from lxml import etree

from elastic.util import parallel


def get_metrics(node):
    cmd = RemoteCommand(node.get_ip(), "sudo /opt/HPCCSystems/bin/testsocket {} '<control:metrics/>'".format(node.get_ip()), capture=True, silent=True)
    cmd.start()

    output_xml = etree.fromstring(cmd.output)
    metrics = {}
    for metric in output_xml.xpath("//Control/Endpoint/Metrics/Metric[@name]"):
        metrics[metric.get('name')] = metric.get('value')
    return metrics


def reset_metrics(node):
    RemoteCommand(node.get_ip(), "sudo /opt/HPCCSystems/bin/testsocket {} '<control:resetMetrics/>'".format(node.get_ip()), capture=True, silent=True).start()
    RemoteCommand(node.get_ip(), "sudo /opt/HPCCSystems/bin/testsocket {} '<control:resetindexmetrics/>'".format(node.get_ip()), capture=True, silent=True).start()
    RemoteCommand(node.get_ip(), "sudo /opt/HPCCSystems/bin/testsocket {} '<control:resetquerystats/>'".format(node.get_ip()), capture=True, silent=True).start()


def get_workload_distribution(node):
    grep_cmd = "grep '{}' /var/log/HPCCSystems/myroxie/roxie.log | wc -l"
    index_cmd = RemoteCommand(node.get_ip(), grep_cmd.format("23CRoxieIndexReadActivity"), capture=True, silent=True)
    fetch_cmd = RemoteCommand(node.get_ip(), grep_cmd.format("19CRoxieFetchActivity"), capture=True, silent=True)
    index_cmd.start()
    fetch_cmd.start()

    return {
        "IndexReadActivity": index_cmd.output,
        "FetchActivity": fetch_cmd.output,
    }


def get_part_access_statistics(node_list):
    with parallel.CommandAgent(concurrency=len(node_list), show_result=False) as agent:
        grep_cmd = "grep '19CRoxieFetchActivity' /var/log/HPCCSystems/myroxie/roxie.log | grep part | cut -d'=' -f3 | while read line; do echo $line | sed 's/.$//'; done | sort | uniq -c"
        agent.submit_remote_commands(node_list, grep_cmd, cids=[n.get_ip() for n in node_list], capture=True, silent=True)

    part_statistics = {}
    for node, cmd in agent.cmd_records.items():
        part_statistics[node] = {}
        for line in cmd.output.splitlines():
            count, part_path = line.strip().split(' ')
            part_statistics[node][part_path] = int(count)
    return part_statistics


def restore_data_placement(nodes, data_dir="/var/lib/HPCCSystems/hpcc-data/roxie"):
    with parallel.CommandAgent(concurrency=len(nodes), show_result=False) as agent:
        cmd = "for d in `find " + data_dir + " -type d`; do echo $d; ls -a $d | grep of | grep '^\.' | cut -c 2- | xargs -I {} sudo mv $d/.{} $d/{}; done"
        agent.submit_remote_commands(nodes, cmd, silent=True)


def switch_data_placement(data_placement, data_dir="/var/lib/HPCCSystems/hpcc-data/roxie"):

    logger = logging.getLogger('.'.join([__name__, "switch_data_placement"]))
    logger.info("Executing data placement")

    def hide_files(nodes, data_dir):
        with parallel.CommandAgent(concurrency=len(nodes), show_result=False) as agent:
            cmd = "for d in `find " + data_dir + " -type d`; do echo $d; ls -F $d | grep -v '[/@=|]$' | sudo xargs -I {} mv $d/{} $d/.{}; done"
            agent.submit_remote_commands(nodes, cmd, silent=True)

    def show_index_files(nodes, data_dir):
        with parallel.CommandAgent(concurrency=len(nodes), show_result=False) as agent:
            cmd = "for d in `find " + data_dir + " -type d`; do echo $d; ls -a $d | grep '^\.idx' | cut -c 2- | xargs -I {} sudo mv $d/.{} $d/{}; done"
            agent.submit_remote_commands(nodes, cmd, silent=True)
    def get_hidden_partition(partition):
        return os.path.dirname(partition) + "/." + os.path.basename(partition)

    hide_files(data_placement.locations.keys(), data_dir=data_dir)
    show_index_files(data_placement.locations.keys(), data_dir=data_dir)
    with parallel.CommandAgent(concurrency=8, show_result=False) as agent:
        for node, partition_list in data_placement.locations.items():
            #logger.info("Host: {}".format(node))
            for partition in partition_list:
                #logger.info("\tpartition={}".format(partition))
                agent.submit_remote_command(node, "sudo mv {} {}".format(get_hidden_partition(partition), partition), capture=False, silent=True)





