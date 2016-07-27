import sys
import os
import glob

layout_complete = {
    '192.168.1.252': [],
    '192.168.1.253': ['*'],
    '192.168.1.254': ['*'],
    '192.168.1.3': ['*'],
    '192.168.1.39': ['*'],
    '192.168.1.4': ['*'],
    '192.168.1.40': ['*'],
    '192.168.1.41': ['*'],
    '192.168.1.42': ['*'],
    '192.168.1.43': [],
}

layout_1r = {
    '192.168.1.252': [],
    '192.168.1.253':
        [
            '*_sorted_people_firstname*'
        ],
    '192.168.1.254':
        [
            '*_sorted_people_lastname*'
        ],
    '192.168.1.3':
        [
            '*_sorted_people_city*'
        ],
    '192.168.1.39':
        [
            '*_sorted_people_zip*'
        ],
    '192.168.1.4':
        [
            '*_unsorted_people_firstname*'
        ],
    '192.168.1.40':
        [
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.41':
        [
            '*_unsorted_people_city*'
        ],
    '192.168.1.42':
        [
            '*_unsorted_people_zip*',
        ],
    '192.168.1.43': [],
}

layout_elastic_4N_1R = {
    '192.168.1.252': [],
    '192.168.1.253':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '192.168.1.254':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.3':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.39':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '192.168.1.4': [],
    '192.168.1.40': [],
    '192.168.1.41': [],
    '192.168.1.42': [],
    '192.168.1.43': [],
}

layout_elastic_8N_2R = {
    '192.168.1.252': [],
    '192.168.1.253':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '192.168.1.254':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.3':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.39':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '192.168.1.4':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '192.168.1.40':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.41':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.42':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '192.168.1.43': [],
}

layout_elastic_8N_xR = {
    '192.168.1.252': [],
    '192.168.1.253':
        [
            '*_sorted_people_firstname*',
            '*_unsorted_people_firstname*'
        ],
    '192.168.1.254':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.3':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.39':
        [
            '*_sorted_people_zip*',
            '*_unsorted_people_zip*'
        ],
    '192.168.1.4':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.40':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.41':
        [
            '*_sorted_people_lastname*',
            '*_unsorted_people_lastname*'
        ],
    '192.168.1.42':
        [
            '*_sorted_people_city*',
            '*_unsorted_people_city*'
        ],
    '192.168.1.43': [],
}


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


def do_switch(node_ip, dataset_dir, layout_table):
    for f in os.listdir(dataset_dir):
        if is_index_file(f):
            continue
        if not is_target_file(f):
            continue
        if is_meta_index_file(f):
            continue
        need_hide = True
        for pattern in layout_table[node_ip]:
            for file_to_show in glob.glob("{}/{}".format(dataset_dir, pattern)):
                if f in file_to_show:
                    need_hide = False
        if need_hide:
            os.rename(os.path.join(dataset_dir, f), os.path.join(dataset_dir, '.{}'.format(f)))


def do_main(node_ip, dataset_dir, layout_table):
    if not os.path.exists(dataset_dir):
        return
    recover_layout(dataset_dir)
    do_switch(node_ip, dataset_dir, layout_table)
    pass


if __name__ == "__main__":
    dataset_dir = "/data/hpcc-data/roxie/mybenchmark"


    node_ip = get_node_ip()
    #do_main(node_ip, dataset_dir, layout_1r)
    do_main(node_ip, dataset_dir, layout_complete)
    #do_main(node_ip, dataset_dir, layout_elastic_4N_1R)
    #do_main(node_ip, dataset_dir, layout_elastic_8N_2R)
    #do_main(node_ip, dataset_dir, layout_elastic_8N_xR)
