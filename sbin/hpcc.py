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
@click.option('--config_dir', type=click.Path(), default="/etc/HPCCSystems")
@click.option('--system_dir', type=click.Path(), default="/opt/HPCCSystems")
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
@click.option('-p', '--package', type=click.Path(exists=True, resolve_path=True))
@click.pass_context
def install_package(ctx, package):
    tmp_path = "/tmp/{}".format(os.path.basename(package))
    for host in ctx.obj['host_list']:
        click.echo('{}: install package {}'.format(host, tmp_path))
        execute("scp  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}".format(package, host, tmp_path), silent=True)
        RemoteCommand(host, "dpkg -i {}; apt-get install -f -y".format(tmp_path), sudo=True, silent=True).start()

@cli.command()
@click.pass_context
def uninstall_package(ctx):
    for host in ctx.obj['host_list']:
        click.echo('{}: uninstall package {}'.format(host, tmp_path))
        RemoteCommand(host, "dpkg -r hpccsystems-platform", sudo=True, silent=True).start()

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
        click.echo('{}: deply configuration'.format(host))
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
