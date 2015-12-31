import getpass

import click
from executor import execute

# getpass.getpass

@click.group()
def cli():
    """This is a command line tool that works for VCL instances
    """

@cli.command()
@click.option('--user', '-m', multiple=True)
def fix_ssh(user):
    """This adds extra users to /etc/ssh/sshd_config.

    If the users are not specified, the current user and hpcc are
    used alternatively.
    """
    user_list = user if (user is not None) and (len(user) > 0)  else [getpass.getuser(), "hpcc"]
    sshd_config = "/etc/ssh/sshd_config"
    execute("cp {} {}.bak".format(sshd_config, sshd_config), sudo=True)
    execute('sed -i "s/^AllowUsers.*/AllowUsers root {}/" {}'.format(" ".join(user_list), sshd_config), sudo=True)
