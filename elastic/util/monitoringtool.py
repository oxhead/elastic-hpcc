import logging

import executor

from elastic.util import parallel
from elastic.util import helper


class Monitor:

    def __init__(self, cluster, output_dir):
        self.cluster = cluster
        self.output_dir = output_dir
        self.output_path = "/tmp/dstat_%s" % helper.get_timestamp()
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def kill_service(self):
        cmd_kill = "ps aux | grep dstat | grep python | tr -s ' ' | cut -d' ' -f2 | xargs kill -9"
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                agent.submit_remote_command(node, cmd_kill, check=False)

    def start(self):
        self.logger.info("Start the monitoring process")
        self.kill_service()
        self.kill_service() # workaround to make sure the dstat is killed
        cmd_start = "nohup dstat -tcly -mg --vm -dr -n --tcp --float --output %s > /dev/null 2>&1 &" % self.output_path
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                agent.submit_remote_command(node, cmd_start, silent=True)

    def stop(self):
        self.logger.info("Stop the monitoring process")
        self.kill_service()
        executor.execute("mkdir -p %s" % self.output_dir)
        for node in self.cluster.get_nodes():
            node_output_file = "%s/dstat_%s.csv" % (self.output_dir, node.get_hostname())
            remote_copy_cmd = "scp %s:%s %s" % (node.get_ip(), self.output_path, node_output_file)
            executor.execute(remote_copy_cmd, check=False, silent=True)