import sys
import os
import glob

import netifaces

node_table = {
    'node1': '10.25.2.146',
    'node2': '10.25.2.147',
    'node3': '10.25.2.148',
    'node4': '10.25.2.149',
    'node5': '10.25.2.151',
    'node6': '10.25.2.152',
    'node7': '10.25.2.153',
    'node8': '10.25.2.157',
    'node9': '10.25.2.131',
    'node10': '10.25.2.132',
}

layout_complete = {
    'node1': [],
    'node2': ['*'],
    'node3': ['*'],
    'node4': ['*'],
    'node5': ['*'],
    'node6': ['*'],
    'node7': ['*'],
    'node8': ['*'],
    'node9': ['*'],
    'node10': [],
}

layout_elastic_4N_1R = {
    'node1': [],
    'node2':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    'node3':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    'node4':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    'node5':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    'node6': [],
    'node7': [],
    'node8': [],
    'node9': [],
    'node10': [],
}

layout_elastic_8N_2R = {
    '10.25.2.146': [],
    '10.25.2.147':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '10.25.2.148':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '10.25.2.149':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '10.25.2.151':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '10.25.2.152':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '10.25.2.153':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '10.25.2.157':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '10.25.2.131':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '10.25.2.132': [],
}

layout_elastic_8N_xR = {
    '10.25.2.146': [],
    '10.25.2.147':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '10.25.2.148':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '10.25.2.149':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '10.25.2.151':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '10.25.2.152':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '10.25.2.153':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '10.25.2.157':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '10.25.2.131':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '10.25.2.132': [],
}


def determin_node_id():
    ip_list = [netifaces.ifaddresses(iface)[netifaces.AF_INET][0]['addr'] for iface in netifaces.interfaces() if netifaces.AF_INET in netifaces.ifaddresses(iface)]
    for ip in ip_list:
        for node_id, node_ip in node_table.items():
            if ip == node_ip:
                return node_id
    raise Exception('Unable to determin the node id for {}'.format(" ".join(ip_list)))

def get_node_ip():
    import socket
    node_ip = socket.gethostbyname(socket.gethostname())
    return node_ip


def is_target_file(file_path):
    return 'data_' in file_path or 'idx_' in file_path

def is_index_file(file_path):
    return 'idx_' in file_path

def is_meta_index_file(file_path):
    if not 'idx_' in file_path:
        return False
    index_number = file_path.split('_')[-3]
    total_number = file_path.split('_')[-1]
    return index_number == total_number


def recover_layout(dataset_dir):
    for f in os.listdir(dataset_dir):
        if is_target_file(f) and f.startswith('.'):
            os.rename(os.path.join(dataset_dir, f), os.path.join(dataset_dir, f[1:]))


def do_switch(node_id, dataset_dir, layout_table):
    for f in os.listdir(dataset_dir):
        if is_index_file(f):
            continue
        if not is_target_file(f):
            continue
        if is_meta_index_file(f):
            continue
        need_hide = True
        for pattern in layout_table[node_id]:
            for file_to_show in glob.glob("{}/{}".format(dataset_dir, pattern)):
                if f in file_to_show:
                    need_hide = False
        if need_hide:
            os.rename(os.path.join(dataset_dir, f), os.path.join(dataset_dir, '.{}'.format(f)))


def do_main(node_id, dataset_dir, layout_table):
    if not os.path.exists(dataset_dir):
        return
    recover_layout(dataset_dir)
    do_switch(node_id, dataset_dir, layout_table)
    pass


if __name__ == "__main__":
    #dataset_dir = "/data/hpcc-data/roxie/mybenchmark"
    dataset_dir = "/var/lib/HPCCSystems/hpcc-data/roxie/mybenchmark"

    node_id = determin_node_id()
    #do_main(node_ip, dataset_dir, layout_1r)
    #do_main(node_id, dataset_dir, layout_complete)
    do_main(node_id, dataset_dir, layout_elastic_4N_1R)
    #do_main(node_ip, dataset_dir, layout_elastic_8N_2R)
    #do_main(node_ip, dataset_dir, layout_elastic_8N_xR)
