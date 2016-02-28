import os
import pickle

import click

@click.group()
@click.option('--hosts', type=click.Path(exists=True, resolve_path=True), default=".cluster_conf")
@click.option('--config_dir', type=click.Path(), default="/etc/HPCCSystems")
@click.option('--system_dir', type=click.Path(), default="/opt/HPCCSystems")
@click.option('--reload/--cached', default=False)
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
        topology = ctx.invoke(cluster_topology)
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
