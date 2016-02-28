import os
import pickle
import random

import click
import executor
from executor import execute

import parallel

def get_word_list():
    return word_list

def get_zipcode_list():
    return zipcode_list

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

@click.group()
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs

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
        cmd = "roxie query validateanagrams --query word {}".format(word)
    elif selected_query == 'searchlinks':
        cmd = "roxie query searchlinks --query name 'Everingham, Andi'"
    elif selected_query == 'fetchpeoplebyzipservice':
        zipcode = random.choice(get_zipcode_list())
        cmd = "roxie query fetchpeoplebyzipservice --query ZIPValue {}".format(zipcode)
    return cmd
