import logging

import executor

from elastic.util import parallel


class HPCCService:
    def __init__(self, cluster):
        self.cluster = cluster
        self.logger = logging.getLogger('.'.join([__name__, self.__class__.__name__]))

    def start(self):
        self.logger.info("Starting HPCC service")
        executor.execute("hpcc service --action start")
        self.logger.info("Started HPCC service")

    def stop(self):
        self.logger.info("Stopping HPCC service")
        executor.execute("hpcc service --action stop")
        self.logger.info("Stopped HPCC service")

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        pass

    def clear_log(self):
        self.logger.info("Clearing HPCC logs")
        executor.execute("hpcc clear_log")

    def truncate_log(self):
        self.logger.info("Truncating HPCC logs")
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_roxie_cluster().get_nodes():
                agent.submit_remote_command(node, "sudo truncate /var/log/HPCCSystems/myroxie/roxie.log --size 0", silent=True)

    def clean_system(self):
        self.logger.info("Clearing HPCC system cache")
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                # dorp file system cache
                agent.submit_remote_command(node, "sudo bash -c 'echo 3 > /proc/sys/vm/drop_caches'", silent=True)
