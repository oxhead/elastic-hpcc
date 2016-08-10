import executor

from elastic.util import parallel


class HPCCService:
    def __init__(self, cluster):
        self.cluster = cluster

    def start(self):
        executor.execute("hpcc service --action start")

    def stop(self):
        executor.execute("hpcc service --action stop")

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        pass

    def clear_log(self):
        executor.execute("hpcc clear_log")

    def clean_system(self):
        with parallel.CommandAgent(len(self.cluster.get_nodes())) as agent:
            for node in self.cluster.get_nodes():
                # dorp file system cache
                agent.submit_remote_command(node, "sudo bash -c 'echo 3 > /proc/sys/vm/drop_caches'", silent=True)
