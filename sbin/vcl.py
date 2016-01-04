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

@cli.command()
@click.option('-u', '--username', prompt=True, default=lambda: os.environ.get('USER', ''))
@click.pass_context
def add_key(ctx, username):
    password = click.prompt("Password", hide_input=True)
    for host in ctx.obj['host_list']:
        click.echo("Adding key to {}".format(host))
        execute("sshpass -p {} ssh-copy-id -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {}@{}".format(password, username, host), silent=True)
        execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ~/.ssh/id_rsa* {}@{}:~/.ssh/".format(username, host), silent=True)

@cli.command()
@click.argument('user')
@click.option('--from_user', default=lambda: os.getenv('USER'))
@click.pass_context
def deploy_key(ctx, user, from_user):
    '''This command deploy the current user's key to a remote user.

    The current implmentation might be insecure and only works for rsa key.  This also assumes the home directory is located at /home.
    '''
    with parallel.CommandAgent(show_result=False) as agent:
        for host in ctx.obj['host_list']:
            agent.submit_command("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null /home/{}/.ssh/id_rsa* {}@{}:/tmp".format(from_user, from_user, host), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo cp /tmp/id_rsa* /home/{}/.ssh; sudo rm -rf /tmp/id_rsa*".format(user), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo bash -c 'cat /home/{}/.ssh/id_rsa.pub >> /home/{}/.ssh/authorized_keys'".format(user, user), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo chmod 600 /home/{}/.ssh/id_rsa*; sudo chmod 644 /home/{}/.ssh/authorized_keys".format(user, user), check=True, silent=True)
    with parallel.CommandAgent(show_result=False) as agent:
        agent.submit_remote_commands(ctx.obj['host_list'], "sudo chown {} /home/{}/.ssh/id_rsa*; sudo chown {} /home/{}/.ssh/authorized_keys".format(user, user, user, user), check=True, silent=True)

@cli.command()
@click.option('--user', '-m', multiple=True)
@click.pass_context
def fix_ssh(ctx, user):
    """This adds extra users to /etc/ssh/sshd_config.

    If the users are not specified, the current user and hpcc are
    used alternatively.
    """
    user_list = user if (user is not None) and (len(user) > 0)  else [getpass.getuser(), "hpcc"]
    for host in ctx.obj['host_list']:
        sshd_config = "/etc/ssh/sshd_config"
        RemoteCommand(host, "sudo cp {} {}.bak".format(sshd_config, sshd_config), ignore_known_hosts=True).start()
        RemoteCommand(host, 'sudo sed -i "s/^AllowUsers.*/AllowUsers root {}/" {}'.format(" ".join(user_list), sshd_config), ignore_known_hosts=True).start()
        RemoteCommand(host, 'sudo service ssh reload', ignore_known_hosts=True).start()

@cli.command()
@click.pass_context
def ip(ctx):
    """This return the internal ip address of the hosts.

    Here we assume the internal ip is bound to eth0.
    """
    for host in ctx.obj['host_list']:
        cmd = RemoteCommand(host, "hostname -I | awk '{print $1}'", ignore_known_hosts=True, capture=True)
        cmd.start()
        click.echo(cmd.output)
