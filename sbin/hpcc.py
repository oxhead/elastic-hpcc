import getpass
import os
import pickle
from collections import defaultdict

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from common import CaptureOutput
import parallel

@click.group()
@click.option('--hosts', type=click.Path(exists=True, resolve_path=True), default=".cluster_conf")
#@click.option('--host', '-m', multiple=True)
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

@cli.command()
@click.pass_context
def test(ctx):
    click.echo(ctx.obj)


@cli.command()
@click.option('-v', '--version')
@click.pass_context
def download_package(ctx, version):
    execute("wget -O hpccsystems-platform-community_5.4.6-1trusty_amd64.deb http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-5.4.6/bin/platform/hpccsystems-platform-community_5.4.6-1trusty_amd64.deb", silent=True)
    #execute("wget -O hpccsystems-platform-community_{}-1trusty_amd64.deb http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-{}/bin/platform/hpccsystems-platform-community_{}-1trusty_amd64.deb".format(version, version, version), silent=True)


@cli.command()
@click.argument('action', type=click.Choice(['install', 'uninstall']))
@click.option('--deb', type=click.Path(exists=True, resolve_path=True))
@click.pass_context
def package(ctx, action, deb):
    if action == 'install':
        tmp_path = "/tmp/{}".format(os.path.basename(deb))
        with parallel.CommandAgent(show_result=False) as agent:
            for host in ctx.obj['host_list']:
                agent.submit_command("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}".format(deb, host, tmp_path), silent=True)
        with parallel.CommandAgent(show_result=False) as agent:
            print(ctx.obj['host_list'])
            agent.submit_remote_commands(ctx.obj['host_list'], "sudo dpkg -i {}; sudo apt-get install -f -y".format(tmp_path), silent=True)
        '''
        for host in ctx.obj['host_list']:
            click.echo('{}: install package {}'.format(host, tmp_path))
            execute("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}".format(package, host, tmp_path), silent=True)
            RemoteCommand(host, "dpkg -i {}; apt-get install -f -y".format(tmp_path), sudo=True, silent=True).start()
        '''
    elif action == 'uninstall':
        with parallel.CommandAgent(show_result=False) as agent:
            agent.submit_remote_commands(ctx.obj['host_list'], "sudo dpkg -r hpccsystems-platform", silent=True)

@cli.command()
@click.pass_context
def clear_log(ctx):
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo rm -rf /var/log/HPCCSystems/*", check=False, silent=True)

@cli.command()
@click.pass_context
def clear_system(ctx):
    # @todo: need to make sure the system is turned off.
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo rm -rf /var/lib/HPCCSystems/*", check=False, silent=True)

@cli.command()
@click.pass_context
def verify_config(ctx):
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "md5sum /etc/HPCCSystems/environment.xml", check=False, silent=False)

@cli.command()
@click.option('-c', '--config', type=click.Path(exists=True, resolve_path=True), default="/etc/HPCCSystems/environment.xml")
@click.pass_context
def deploy_config(ctx, config):
    for host in ctx.obj['host_list']:
        click.echo('{}: deploy configuration'.format(host))
        RemoteCommand(host, "cp {}/environment.xml {}/environment.xml.bak".format(ctx.obj['config_dir'], ctx.obj['config_dir']), ignore_known_hosts=True, sudo=True).start()
        execute("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:/tmp/environment.xml".format(config, host), silent=True)
        RemoteCommand(host, "cp /tmp/environment.xml {}/environment.xml".format(ctx.obj['config_dir']), sudo=True, silent=True).start()


@cli.command()
@click.option('--action', type=click.Choice(['start', 'stop', 'status', 'list_components', 'list_types']))
@click.option('-c', '--component', multiple=True, type=click.Choice(['dafilesrv', 'dali', 'dfuserver', 'eclagent', 'eclccserver', 'eclscheduler', 'esp', 'sasha', 'thor', 'roxie']))
@click.pass_context
def service(ctx, action, component):
    if action == 'list_components':
        with parallel.CommandAgent() as agent:
            cmd = "sudo service hpcc-init --componentlist"
            agent.submit_remote_commands(ctx.obj['host_list'], cmd, check=False, silent=False)
    elif action == 'list_types':
        with parallel.CommandAgent() as agent:
            cmd = "sudo service hpcc-init --typelist"
            agent.submit_remote_commands(ctx.obj['host_list'], cmd, check=False, silent=False)
    else:
        if len(component) > 0:
            filtered_components = [n for n in component if n is not "dafilesrv"]
            if len(filtered_components) > 0:
                cmd = "sudo service hpcc-init {} {}".format(" ".join(["-c %s" % n for n in component]), action)
                with parallel.CommandAgent() as agent:
                    agent.submit_remote_commands(ctx.obj['host_list'], cmd, check=False, silent=False)
                if 'dafilesrv' in component:
                    cmd = "sudo service dafilesrv {}".format(action)
                    with parallel.CommandAgent() as agent:
                        agent.submit_remote_commands(ctx.obj['host_list'], cmd, check=False, silent=False)
        else:
            with parallel.CommandAgent() as agent:
                agent.submit_remote_commands(ctx.obj['host_list'], "sudo service hpcc-init {}".format(action), check=False, silent=True, capture=True)
            with parallel.CommandAgent() as agent:
                agent.submit_remote_commands(ctx.obj['host_list'], "sudo service dafilesrv {}".format(action), check=False, silent=True, capture=True)

@cli.command()
@click.option('-u', '--username', default='hpcc')
@click.pass_context
def deploy_key(ctx, username):
    '''This command deploy the current user's key to a remote user.

    The current implmentation might be insecure.
    '''
    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            agent.submit_command("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ~/.ssh/id_rsa* {}@{}:/tmp".format(username, host), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo cp /tmp/id_rsa* /home/{}/.ssh; sudo rm -rf /tmp/id_rsa*".format(username), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo cat /home/{}/.ssh/id_rsa.pub >> /home/{}/.ssh/authorized_keys".format(username, username), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo chmod 644 /home/{}/.ssh/*".format(username), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo chown {} /home/{}/.ssh/*".format(username, username), check=True, silent=True)

@cli.command()
@click.pass_context
def cluster_topology(ctx):
    topology = defaultdict(lambda : [])
    with CaptureOutput() as output:
        ctx.invoke(service, action='status')
    host = None
    for line in output:
        if '[' in line:
            host = line.split('] ')[-1]
        elif len(line) > 0:
            component = line.split(' ')[0].replace('my', '')
            running = 'running' in line
            topology[component].append((host, running))
    #print(topology)
    return dict(topology)

@cli.command()
@click.option('--data', type=click.Path(exists=True, resolve_path=True))
@click.option('--dropzone_path', default='/var/lib/HPCCSystems/mydropzone')
@click.pass_context
def upload_data(ctx, data, dropzone_path):
    click.echo('upload data')
    topology = ctx.invoke(cluster_topology)
    landing_zone_host, component_status = topology['dfuserver'][0]
    # todo: fix the file permissino in the landingzone server
    execute('bash -c "scp -r {} {}:{}"'.format(data, landing_zone_host, dropzone_path))

@cli.command()
@click.argument('data')
@click.argument('dstname')
@click.option('--dstcluster', default='myroxie')
@click.option('--format', default='fixed', type=click.Choice(['fixed', 'csv', 'xml', 'json']))
@click.option('--recordsize', type=int)
@click.pass_context
def spray(ctx, data, dstname, dstcluster, format, recordsize):
    click.echo('runing roxie query')
    #topology = ctx.invoke(cluster_topology)
    dali_host, component_status = ctx.obj['topology']['dali'][0]
    execute('{}/bin/dfuplus server={} action=spray srcip={} srcfile=/var/lib/HPCCSystems/mydropzone/{} dstname={} dstcluster={} format={} recordsize={}'.format(ctx.obj['system_dir'], dali_host, dali_host, data, dstname, dstcluster, format, recordsize))
#bin/dfuplus action=spray srcip=10.25.11.81 srcfile=/var/lib/HPCCSystems/mydropzone/OriginalPerson dstname=tutorial:YN::OriginalPerson dstcluster=mythor format=fixed recordsize=124 server=152.46.16.135

@cli.command()
@click.option('--ecl', type=click.Path(exists=False, resolve_path=False))
@click.pass_context
def roxie(ctx, ecl):
    click.echo('runing roxie query')
    thor_master_host, component_status = ctx.obj['topology']['thor'][0]
    with parallel.CommandAgent(show_result=False, concurrency=1) as agent:
        # assume in the roxie dir for now
        agent.submit_remote_command(thor_master_host, 'cd roxie; ecl run --target roxie {}'.format(ecl))
