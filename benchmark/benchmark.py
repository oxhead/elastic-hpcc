import os
import pickle
import random

import click
import executor
from executor import execute
import yaml

import parallel
from ecl import convert_to_query_xml
from elastic.benchmark.zeromqimpl import *
from elastic.util import network as network_util

def get_word_list():
    return word_list

def get_zipcode_list():
    return zipcode_list

def get_actor_list():
    return actor_list

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

word_file = os.path.join(get_script_dir(), 'dataset', 'word_list.txt')
word_list = []
with open(word_file) as f:
    word_list = f.read().splitlines()

zipcode_file = os.path.join(get_script_dir(), 'dataset', 'zipcode_list.txt')
zipcode_list = []
with open(zipcode_file) as f:
    zipcode_list = f.read().splitlines()

actor_file = os.path.join(get_script_dir(), 'dataset', 'actor_list.txt')
actor_list = []
with open(actor_file) as f:
    actor_list = [actor.rstrip() for actor in f.read().splitlines()]


@click.group()
@click.option('--config', type=click.Path(exists=True, resolve_path=True), default="conf/benchmark.yaml")
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs
    if ctx.obj['config'] is not None:
        ctx.obj['_config'] = BenchmarkConfig.parse_file(ctx.obj['config'])

@cli.command()
@click.option('--dataset', multiple=True, default=['anagram2', 'originalperson', 'sixdegree'])
@click.pass_context
def download_dataset(ctx, dataset):
    print(dataset)
    dataset_dir = os.path.join(get_script_dir(), "dataset")
    if 'anagram2' in dataset:
        anagram2_dataset_dir = os.path.join(dataset_dir, "Anagram2")
        execute("mkdir -p {}".format(anagram2_dataset_dir))
        execute("wget http://downloads.sourceforge.net/wordlist/12dicts-5.0.zip -O {}/12dicts-5.0.zip".format(anagram2_dataset_dir))
        execute("unzip {}/12dicts-5.0.zip -d {}".format(anagram2_dataset_dir, anagram2_dataset_dir))
    elif 'originalperson' in dataset:
        originalperson_dataset_dir = os.path.join(dataset_dir, "OriginalPerson")
        execute("mkdir -p {}".format(originalperson_dataset_dir))
        execute("wget http://wpc.423a.rhocdn.net/00423A/install/docs/3_8_0_8rc_CE/OriginalPerson -O {}/OriginalPerson".format(originalperson_dataset_dir))
    elif 'sixedegree' in dataset:
        sixdegree_dataset_dir = os.path.join(dataset_dir, "SixDegree")
        execute("mkdir -p {}".format(sixdegree_dataset_dir))
        execute("wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actors.list.gz -O {}/actors.list.gz".format(sixdegree_dataset_dir))
        execute("wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actresses.list.gz -O {}/actresses.list.gz".format(sixdegree_dataset_dir)) 
        execute("zcat {}/actors.list.gz > {}/actors.list".format(sixdegree_dataset_dir, sixdegree_dataset_dir))
        execute("zcat {}/actresses.list.gz > {}/actresses.list".format(sixdegree_dataset_dir, sixdegree_dataset_dir))


@cli.command()
@click.option('--action', type=click.Choice(['start', 'stop', 'status']))
@click.pass_context
def service(ctx, action):
    if action == "start":
         with parallel.CommandAgent(show_result=False) as agent:
             # TODO: a better way? Should not use fixed directory
             agent.submit_remote_command(ctx.obj['_config'].get_controller(), 'bash ~/elastic-hpcc/script/start_controller.sh')
             for driver_node in ctx.obj['_config'].get_drivers():
                 print(driver_node)
                 agent.submit_remote_command(driver_node, 'bash ~/elastic-hpcc/script/start_driver.sh')
    else:
        commander = BenchmarkCommander(ctx.obj['_config'].get_controller(), ctx.obj['_config'].lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT))
        if action == "stop":
            commander.stop()
        elif action == "status":
            print(commander.status())
        

@cli.command()
@click.option('-n', '--node', multiple=True)
@click.pass_context
def install_package(ctx, node):
    deploy_set = set()
    if not network_util.is_local_ip(ctx.obj['_config'].get_controller()):
        deploy_set.add(ctx.obj['_config'].get_controller())
    for driver_node in ctx.obj['_config'].get_drivers():
        if not network_util.is_local_ip(driver_node):
            deploy_set.add(driver_node)
    for host in deploy_set:
        with parallel.CommandAgent(concurrency=len(host), show_result=False) as agent:
            agent.submit_remote_command(host, "cd ~/elastic-hpcc; source init.sh")

@cli.command()
@click.option('-n', '--node', multiple=True)
@click.pass_context
def deploy(ctx, node):
    deploy_set = set()
    if not network_util.is_local_ip(ctx.obj['_config'].get_controller()):
        deploy_set.add(ctx.obj['_config'].get_controller())
    for driver_node in ctx.obj['_config'].get_drivers():
        if not network_util.is_local_ip(driver_node):
            deploy_set.add(driver_node)
    for host in deploy_set:
        with parallel.CommandAgent(concurrency=len(host), show_result=False) as agent:
             # TODO: a better way? Should not use fixed directory
             agent.submit_command('rsync -avz --exclude elastic-hpcc/HPCC-Platform --exclude elastic-hpcc/benchmark --exclude elastic-hpcc/.git --exclude elastic-hpcc/.venv ~/elastic-hpcc {}:~/'.format(host)) 
    
        with parallel.CommandAgent(concurrency=len(host), show_result=False) as agent:
             agent.submit_command('rsync ~/elastic-hpcc/benchmark/*.py {}:~/elastic-hpcc/benchmark'.format(host))
    

@cli.command()
@click.option('--application', multiple=True, type=click.Choice(['validateanagrams', 'searchlinks', 'fetchpeoplebyzipservice']))
@click.option('--num_quries', type=int, default=100)
@click.pass_context
def distributed_run(ctx, **kwargs):
    ctx.config['applications'] = kwargs['application']
    ctx.config['num_quries'] = kwargs['num_quries']
