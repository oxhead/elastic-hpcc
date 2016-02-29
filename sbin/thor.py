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

from cli import cli
from ecl import run as ecl_run

@cli.command()
@click.option('--ecl', type=click.Path(exists=True, resolve_path=True))
@click.option('--query', '-q', multiple=True, type=(str, str))
@click.option('--dir', '-d', type=click.Path(exists=True, resolve_path=True))
@click.option('--job')
@click.option('--wait', type=int, default=30000)
@click.option('--wait_until_complete', is_flag=True)
@click.pass_context
def run(ctx, ecl, query, dir, job, wait, wait_until_complete):
    ctx.forward(ecl_run, target='thor', job=job, wait=wait, wait_until_complete=wait_until_complete)
