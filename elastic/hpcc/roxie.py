from executor import execute
from executor.ssh.client import RemoteCommand
from lxml import etree


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





