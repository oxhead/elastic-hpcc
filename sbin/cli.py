from collections import defaultdict
import os
import pickle

import click

from common import CaptureOutput
import parallel

@click.group()
@click.option('--hosts', type=click.Path(exists=True, resolve_path=True), default=".cluster_conf")
@click.option('--config_dir', type=click.Path(), default="/etc/HPCCSystems")
@click.option('--system_dir', type=click.Path(), default="/opt/HPCCSystems")
@click.option('--reload/--cached', default=False)
@click.option('--show/--silent', default=False)
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs
    ctx.obj['host_list'] = []
    if ctx.obj['hosts'] is not None:
        with open(ctx.obj['hosts']) as f:
            ctx.obj['host_list'].extend([line.rstrip('\n') for line in f])
    elif len(ctx.obj['host']) > 0:
        ctx.obj['host_list'].extend(ctx.obj['host'])
    topology_cached = "/tmp/.topology.cached"
    if ctx.obj['reload'] or (not os.path.exists(topology_cached)):
        topology = create_cluster_topology(ctx)
        print(topology)
        ctx.obj['topology'] = topology
        with open(topology_cached, 'wb') as f:
            pickle.dump(topology, f)
    else:
        with open(topology_cached, 'rb') as f:
            ctx.obj['topology'] = pickle.load(f)

def get_system_dir(ctx):
    return ctx.obj['system_dir']

def get_thor(ctx):
    thor_master_host, component_status = ctx.obj['topology']['roxie'][0]
    return thor_master_host

def get_roxie(ctx):
    eclagent_host, component_status = ctx.obj['topology']['eclagent'][0]
    return eclagent_host

def get_esp(ctx):
    eclagent_host, component_status = ctx.obj['topology']['eclagent'][0]
    return eclagent_host

def create_cluster_topology(ctx):
    topology = defaultdict(lambda : [])
    with CaptureOutput() as output:
        with parallel.CommandAgent() as agent:
                agent.submit_remote_commands(ctx.obj['host_list'], "sudo service hpcc-init {}".format("status"), check=False, silent=True, capture=True)
    host = None
    for line in output:
        if '[' in line:
            host = line.split('] ')[-1]
        elif len(line) > 0:
            component = line.split(' ')[0].replace('my', '')
            running = 'running' in line
            topology[component].append((host, running))
    return dict(topology)
