import getpass
import os

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

import parallel

@click.group()
@click.option('--hosts', type=click.Path(exists=True, resolve_path=True))
@click.option('--host', '-m', multiple=True)
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


@click.argument('cmdline')
@click.pass_context
def cmd(ctx, cmdline):
    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            agent.submit_remote_command(host, cmdline, ignore_known_hosts=True, check=False)


@cli.command()
@click.option('-p', '--prefix', default='node')
@click.pass_context
def create_hosts(ctx, prefix):
    preload_config = '''127.0.0.1 localhost

# The following lines are desirable for IPv6 capable hosts
::1 ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts'''

    tmp_file = '/tmp/.hosts'

    with open(tmp_file, 'w') as f:
        f.write(preload_config)
        f.write('\n\n')
        for i in range(len(ctx.obj['host_list'])):
            host = ctx.obj['host_list'][i]
            f.write("{} {}{}\n".format(host, prefix, i+1))

    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            agent.submit_command("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}".format(tmp_file, host, tmp_file), check=False)

    with parallel.CommandAgent(show_result=False) as agent:
        for i in range(len(ctx.obj['host_list'])):
            host = ctx.obj['host_list'][i]
            agent.submit_remote_command(host, "sudo hostname {}{}".format(prefix, i+1), silent=True, check=True)

    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            agent.submit_remote_command(host, "sudo cp {} /etc/hosts".format(tmp_file), silent=True, check=True)
