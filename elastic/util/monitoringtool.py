import sys
import os
import datetime
import time
import logging

import executor
from executor.ssh.client import RemoteCommand
from executor import ExternalCommand

from elastic.util import parallel
from elastic.util import helper

logger = logging.getLogger(__name__)

class Monitor():

    def __init__(self, cluster, output_dir):
        self.cluster = cluster
        self.output_dir = output_dir
        self.output_path = "/tmp/dstat_%s" % helper.get_timestamp()

    @property
    def logger(self):
        name = '.'.join([__name__, self.__class__.__name__])
        return logging.getLogger(name)

    def kill_service(self):
        cmd_kill = "ps aux | grep dstat | grep python | tr -s ' ' | cut -d' ' -f2 | xargs kill -9"
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                agent.submit_remote_command(node, cmd_kill, check=False)

    def start(self):
        logger.debug("Start the monitoring process")
        self.kill_service()
        self.kill_service() # workaround to make sure the dstat is killed
        cmd_start = "nohup dstat -tcly -mg --vm -dr -n --tcp --float --output %s > /dev/null 2>&1 &" % self.output_path
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                agent.submit_remote_command(node, cmd_start)

    def stop(self):
        logger.debug("Stop the monitoring process")
        self.kill_service()
        executor.execute("mkdir -p %s" % self.output_dir)
        for node in self.cluster.get_nodes():
            node_output_file = "%s/dstat_%s.csv" % (self.output_dir, node.get_hostname())
            remote_copy_cmd = "scp %s:%s %s" % (node.get_hostname(), self.output_path, node_output_file)
            executor.execute(remote_copy_cmd, check=False, silent=True)