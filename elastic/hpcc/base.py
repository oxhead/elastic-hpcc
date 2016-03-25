class Cluster(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.esp = None
        self.roxies = []
        self.thors = []
    def set_esp(self, node):
        self.esp = node
    def get_esp(self, node):
        return self.esp
    def add_roxie(self, node):
        self.roxies.append(node)
    def add_thor(self, node):
        self.thors.append(node)
    def get_nodes(self):
        s = set()
        s.update(self.roxie)
        s.update(self.thors)
        return sorted(list(s), key=lambda node: node.hostname)
    def get_roxies(self): 
        return sorted(self.mons, key=lambda node: node.hostname)
    def get_thors(self):
        return sorted(self.osds, key=lambda node: node.hostname)
    def get_conf(self):
        return self.kwargs['hpcc_conf']
