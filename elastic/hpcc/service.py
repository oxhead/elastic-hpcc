import executor


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
