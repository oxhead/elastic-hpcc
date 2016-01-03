import getpass
import os

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand

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
    for host in reversed(ctx.obj['host_list']):
        click.echo('{}: service {}'.format(host, action if action is not None else "lookup"))
        if action == 'list_components':
            RemoteCommand(host, "sudo service hpcc-init --componentlist", check=False, silent=False).start()
        elif action == 'list_types':
            RemoteCommand(host, "sudo service hpcc-init --typelist", check=False, silent=False).start()
        else: 
            if len(component) > 0:
                filtered_components = [n for n in component if n is not "dafilesrv"]
                if len(filtered_components) > 0:
                    RemoteCommand(host, "sudo service hpcc-init {} {}".format(" ".join(["-c %s" % n for n in component]), action), check=False, silent=False).start()
                if 'dafilesrv' in component:
                    RemoteCommand(host, "sudo service dafilesrv {}".format(action), check=False, silent=False).start()
            else:
                RemoteCommand(host, "sudo service hpcc-init {}".format(action), check=False, silent=False).start()
                RemoteCommand(host, "sudo service dafilesrv {}".format(action), check=False, silent=False).start()

@cli.command()
@click.option('--action', type=click.Choice(['show', 'clear']))
@click.pass_context
def log(ctx, action):
    pass
