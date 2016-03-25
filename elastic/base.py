class Node(object):
    def __init__(self, hostname, ip):
        self.hostname = hostname
        self.ip = ip
    def get_hostname(self):
        return self.hostname
    def get_ip(self):
        return self.ip
    def __eq__(self, other):
        return (isinstance(other, self.__class__) and self.ip == other.ip)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __repr__(self):
        return "%s(%s)" % (self.hostname, self.ip)
    def __hash__(self):
        return hash(self.__repr__())
