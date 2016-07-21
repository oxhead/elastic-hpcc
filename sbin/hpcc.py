import getpass
import os
import pickle
from collections import defaultdict
import shlex

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

from common import CaptureOutput
import parallel

from cli import cli


def get_system_dir(ctx):
    return ctx.obj['system_dir']

def get_thor(ctx):
    thor_master_host, component_status = ctx.obj['topology']['roxie'][0]
    return thor_master_host

def get_roxie(ctx):
    eclagent_host, component_status = ctx.obj['topology']['eclagent'][0]
    return eclagent_host

@cli.command()
@click.option('-v', '--version')
@click.pass_context
def download_package(ctx, version):
    execute("wget -O hpccsystems-platform-community_5.4.6-1trusty_amd64.deb http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-5.4.6/bin/platform/hpccsystems-platform-community_5.4.6-1trusty_amd64.deb", silent=True)
    #execute("wget -O hpccsystems-platform-community_{}-1trusty_amd64.deb http://wpc.423a.rhocdn.net/00423A/releases/CE-Candidate-{}/bin/platform/hpccsystems-platform-community_{}-1trusty_amd64.deb".format(version, version, version), silent=True)


@cli.command()
@click.argument('action', type=click.Choice(['install', 'uninstall', 'fix']))
@click.option('--deb', type=click.Path(exists=True, resolve_path=True))
@click.pass_context
def package(ctx, action, deb):
    if action == 'install':
        tmp_path = "/tmp/{}".format(os.path.basename(deb))
        with parallel.CommandAgent(show_result=False) as agent:
            for host in ctx.obj['host_list']:
                agent.submit_command("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}".format(deb, host, tmp_path), silent=True)
        # restrict the number of concurrency to avoid blocked by the APT system durning installation
        with parallel.CommandAgent(show_result=False, concurrency=4) as agent:
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
    elif action == 'fix':
        tmp_path = "/tmp/{}".format(os.path.basename(deb))
        with parallel.CommandAgent(show_result=False, concurrency=1) as agent:
            agent.submit_remote_commands(ctx.obj['host_list'], "sudo dpkg -r hpccsystems-platform; sudo dpkg -i {}; sudo apt-get install -f -y".format(tmp_path), silent=True)

@cli.command()
@click.argument('command')
@click.pass_context
def cmd(ctx, command):
    # @todo: need to make sure the system is turned off.
    with parallel.CommandAgent(show_result=False, concurrency=1) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], command, check=False, silent=False)

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
@click.option('-c', '--config', type=click.Path(exists=True, resolve_path=True), default="/etc/HPCCSystems/source/mycluster.xml", help='The default configuration is located at /etc/HPCCSystems/source/mycluster.xml.')
@click.pass_context
def deploy_config(ctx, config):
    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            RemoteCommand(host, "cp {}/environment.xml {}/environment.xml.bak".format(ctx.obj['config_dir'], ctx.obj['config_dir']), ignore_known_hosts=True, sudo=True).start()
            agent.submit_command("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:/tmp/environment.xml".format(config, host), silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            click.echo('{}: deploy configuration'.format(host))
            agent.submit_remote_command(host, "cp /tmp/environment.xml {}/environment.xml".format(ctx.obj['config_dir']), sudo=True, silent=True)


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
            # needs to start the master for avoiding failure when the cluster size is more than 8
            #if action == 'start':
            #    RemoteCommand(ctx.obj['topology']['esp'][0][0], "sudo service hpcc-init {}".format(action), silent=False, check=True).start()
            with parallel.CommandAgent() as agent:
                    agent.submit_remote_commands(ctx.obj['host_list'], "sudo service hpcc-init {}".format(action), check=False, silent=True, capture=True)
            if action == 'stop':
                with parallel.CommandAgent() as agent:
                    agent.submit_remote_commands(ctx.obj['host_list'], "sudo pkill -9 dafilesrv; sudo pkill -9 roxie", check=False, silent=True, capture=True)

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
def cluster_topology(ctx, show=False):
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
    if show:
        for (component, hosts) in topology.items():
            print(component)
            for (host, condition) in hosts:
                print("\t{}: {}".format(host, condition))
    return dict(topology)


@cli.command()
@click.pass_context
def print_topology(ctx):
    print(ctx.obj['topology'])


@cli.command()
@click.option('--data', type=click.Path(exists=True, resolve_path=True))
@click.option('--dropzone_path', default='/var/lib/HPCCSystems/mydropzone')
@click.pass_context
def upload_data(ctx, data, dropzone_path):
    click.echo('upload data')
    topology = ctx.invoke(cluster_topology, show=False)
    landing_zone_host, component_status = topology['dfuserver'][0]
    # todo: fix the file permissino in the landingzone server
    execute('bash -c "scp -r {} {}:{}"'.format(data, landing_zone_host, dropzone_path))

@cli.command()
@click.option('--data', type=click.Path(exists=True, resolve_path=True))
@click.option('--dropzone_path', default='/var/lib/HPCCSystems/mydropzone')
@click.pass_context
def list_data(ctx, data, dropzone_path):
    click.echo('list logical data')
    eclagent_host = get_roxie(ctx)
    cmd = "{}/bin/dfuplus server={} action=list".format(get_system_dir(ctx), eclagent_host)
    output = execute(cmd, capture=True, silent=True)
    file_list = output.splitlines()[1:]
    for f in file_list:
        print(f)
    return file_list

@cli.command()
@click.option('--data', multiple=True)
@click.pass_context
def remove_data(ctx, data):
    click.echo('remove logical files')
    eclagent_host = get_roxie(ctx)
    for f in data:
        cmd = "{}/bin/dfuplus server={} action=remove name={}".format(get_system_dir(ctx), eclagent_host, f)
        execute(cmd)


@cli.command()
@click.confirmation_option(help='Are you sure you want to delete these files?')
@click.pass_context
def clean_data(ctx):
    click.echo('remove logical files')
    eclagent_host = get_roxie(ctx)
    file_list = []
    with CaptureOutput() as output:
        file_list = ctx.invoke(list_data)
    for f in file_list:
        cmd = "{}/bin/dfuplus server={} action=remove name={}".format(get_system_dir(ctx), eclagent_host, f)
        execute(cmd)

@cli.command()
@click.argument('data')
@click.argument('dstname')
@click.option('--dstcluster', default='myroxie')
@click.option('--format', default='fixed', type=click.Choice(['fixed', 'csv', 'delimited', 'xml', 'json', 'variable', 'recfmv', 'recfmvb']))
@click.option('--recordsize', type=int, default=None)
@click.option('--maxrecordsize', type=int, default=None)
@click.option('--separator', default=None)
@click.option('--terminator', default=None)
@click.option('--quote', default=None)
@click.option('--overwrite', is_flag=True)
@click.option('--replicate', is_flag=True)
@click.pass_context
def spray(ctx, data, dstname, **kwargs):
    click.echo('runing roxie query')
    args = []
    for (k, v) in kwargs.items():
        if v is not None:
            if type(v) == bool:
                args.append("{}={}".format(k, "1" if bool(v) else "0"))
            else:
                args.append("{}={}".format(k, shlex.quote(str(v))))
    dali_host, component_status = ctx.obj['topology']['dali'][0]
    cmd = '{}/bin/dfuplus server={} action=spray srcip={} srcfile=/var/lib/HPCCSystems/mydropzone/{} dstname={} {}'.format(ctx.obj['system_dir'], dali_host, dali_host, data, dstname, " ".join(args))
    execute(cmd)
    #with parallel.CommandAgent(show_result=False, concurrency=1) as agent:
    #    agent.submit_command(cmd)

@cli.command()
@click.option('--output', default='/etc/HPCCSystems/source/mycluster.xml', type=click.Path(exists=None, resolve_path=True))
@click.option('--thor', default=1, type=int, help='The number of Thor nodes')
@click.option('--roxie', default=1, type=int, help='The number of roxie nodes')
@click.option('--support', default=1, type=int, help='The number of support nodes')
@click.option('--num_replica', default=2, type=int, help='The number of replicas')
@click.option('--channel_mode', default='simple', type=click.Choice(['simple', 'cyclic', 'overloaded', 'elastic']), help='The mechanism to determine how the channel assignment')
@click.option('--attribute', '-a', multiple=True, type=(str, str, str), help='The custormized configurations in XML via XPath')
@click.option('--overwrite', is_flag=True)
@click.pass_context
def gen_config(ctx, output, thor, roxie, support, num_replica, channel_mode, attribute, overwrite):
    if not overwrite and os.path.exists(output):
        print('The output file already exists')
        return

    if channel_mode == 'simple':
        channel_mode_str = 'full redundancy'
    elif channel_mode == 'cyclic':
        channel_mode_str = 'cyclic redundancy'
    else:
        channel_mode_str = channel_mode

    customized_attr_list = ""
    for (xpath, attr, value) in attribute:
        customized_attr_list = customized_attr_list + " -set_xpath_attrib_value {} {} '{}' ".format(xpath, attr, value)
    if channel_mode is not None:
        customized_attr_list = customized_attr_list + " -set_xpath_attrib_value Software/RoxieCluster @slaveConfig '{}' ".format(channel_mode_str)
    if num_replica is not None:
        customized_attr_list = customized_attr_list + " -set_xpath_attrib_value Software/RoxieCluster @numDataCopies '{}' ".format(num_replica)

    cmd = "{}/sbin/envgen -env {} -ipfile {} -supportnodes {} -thornodes {} -roxienodes {} -slavesPerNode {} {}".format(get_system_dir(ctx), output, ctx.obj['hosts'], support, thor, roxie, 1, customized_attr_list)
    print(cmd)
    execute(cmd)
