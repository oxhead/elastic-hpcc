import netifaces

def is_local_ip(ip):
    for interface in netifaces.interfaces():
        addrs = netifaces.ifaddresses(interface)
        if ip == addrs[netifaces.AF_INET][0]['addr']:
            return True
    return False
