import os
import pickle
import random

import click
import executor
from executor import execute

import parallel
from ecl import convert_to_query_xml

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
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs

@cli.command()
@click.option('--dataset', multiple=True, default=['anagram2', 'originalperson', 'sixdegree'])
@click.pass_context
def download_dataset(ctx, dataset):
    print(dataset)
    return
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
@click.option('--times', type=int, default=100)
@click.option('--concurrency', type=int, default=1)
@click.option('--query', multiple=True, type=click.Choice(['validateanagrams', 'searchlinks', 'fetchpeoplebyzipservice']))
@click.pass_context
def stress(ctx, times, concurrency, query):
    click.echo('Benchmark: stress test')
    for i in range(times):
        agent = parallel.CommandAgent(show_result=False, concurrency=concurrency)
        query_records = []
        for j in range(concurrency):
            query_cmd = random_query(query)
            cmd_id = "i{}c{}".format(i, j) 
            agent.submit(cmd_id, query_cmd, silent=True)
            query_records.append((cmd_id, query_cmd))
        agent.run()
        status_records = agent.status()
        for j in range(len(query_records)):
            (cmd_id, selected_query) = query_records[j]
            print("Iteration {}, Thread {}: {}".format(i+1, j+1, status_records[cmd_id]))
            print("\t{}".format(selected_query))
        #print("[{}] {}: {}".format(i+1, selected_query, status))
        #print("\t{}".format(cmd))

def random_query(query_list):
    selected_query = random.choice(query_list)
    cmd = None
    if selected_query == 'validateanagrams':
        word = random.choice(get_word_list())
        cmd = "roxie query validateanagrams --input word {}".format(word)
    elif selected_query == 'searchlinks':
        actor = random.choice(get_actor_list())
        cmd = "roxie query searchlinks --input name '{}'".format(actor)
    elif selected_query == 'fetchpeoplebyzipservice':
        zipcode = random.choice(get_zipcode_list())
        cmd = "roxie query fetchpeoplebyzipservice --input ZIPValue {}".format(zipcode)
    return cmd

@cli.command()
@click.option('--hosts', type=click.Path(exists=True, resolve_path=True), default='.benchmark_hosts')
@click.option('--query', type=click.Choice(['validateanagrams', 'searchlinks', 'fetchpeoplebyzipservice']))
@click.option('--input', '-i', multiple=True, type=(str, str))
@click.option('--times', type=int, default=100)
@click.option('--concurrency', type=int, default=1)
@click.pass_context
def distributed_stress(ctx, hosts, query, input, times, concurrency):
    num_hosts = None
    with open(hosts, 'r') as f:
        num_hosts = len(f.read().splitlines())
    if num_hosts < concurrency:
        click.echo('too few machines to run benchmark')
        ctx.abort()
    selected_hosts = []
    with open(hosts, 'r') as f:
        selected_hosts = random.sample(f.read().splitlines(), concurrency)

    input_str = convert_to_query_xml(input)
    with parallel.CommandAgent(show_result=False, concurrency=num_hosts) as agent: 
        for host in selected_hosts:
            print(host)
            print('for (( i=0;i<{};i++ )); do /opt/HPCCSystems/bin/ecl run roxie --server=10.25.11.81 {} {} >> /tmp/results.txt; done'.format(times, query, input_str))
            agent.submit_remote_command(host, 'for (( i=0;i<{};i++ )); do /opt/HPCCSystems/bin/ecl run roxie --server=10.25.11.81 {} {} >> /tmp/results.txt; done'.format(times, query, input_str), silent=True, capture=False)
