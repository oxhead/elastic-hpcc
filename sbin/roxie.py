import getpass
import os
import pickle
from collections import defaultdict

import click
import executor
from executor import execute
from executor.ssh.client import RemoteCommand
from lxml import etree

from common import CaptureOutput
import parallel
from cli import cli
from cli import get_roxie
from cli import get_system_dir
from ecl import run as ecl_run
from ecl import convert_to_query_xml

@cli.command()
@click.option('--ecl', type=click.Path(exists=True, resolve_path=True))
@click.option('--dir', '-d', type=click.Path(exists=True, resolve_path=True))
@click.option('--input', '-i', multiple=True, type=(str, str))
@click.option('--wait', type=int, default=30000)
@click.option('--job')
@click.option('--wait_until_complete', is_flag=True)
@click.pass_context
def run(ctx, ecl, dir, input, wait, job, wait_until_complete):
    ctx.forward(ecl_run, target='roxie')

@cli.command()
@click.argument('name')
@click.option('--ecl', type=click.Path(exists=True, resolve_path=True))
@click.option('--dir', '-d', type=click.Path(exists=True, resolve_path=True))
@click.pass_context
def publish(ctx, name, ecl, dir):
    click.echo('publishing a Roxie query')
    eclagent_host = get_roxie(ctx)
    program_dir = os.path.dirname(ecl) if dir is None else dir
    cmd = 'cd {}; {}/bin/ecl publish roxie {} --name={} --server={} -A'.format(program_dir, get_system_dir(ctx), ecl, name, eclagent_host)
    execute(cmd, sudo=True)

@cli.command()
@click.argument('name')
@click.pass_context
def unpublish(ctx, name):
    click.echo('unpublishing a Roxie query')
    eclagent_host = get_roxie(ctx)
    cmd = '{}/bin/ecl unpublish roxie {} --server={}'.format(get_system_dir(ctx), name, eclagent_host)
    execute(cmd, sudo=True)


@cli.command()
@click.argument('name')
@click.option('--input', '-i', multiple=True, type=(str, str))
@click.option('--job')
@click.option('--wait', type=int, default=30000)
@click.option('--wait_until_complete', is_flag=True)
@click.pass_context
def query(ctx, name, input, job, wait, wait_until_complete):
    return ctx.forward(ecl_run, name=name, target='roxie')

@cli.command()
@click.argument('wuid')
@click.pass_context
def lookup_workunit_info(ctx, wuid):
    eclagent_host = get_roxie(ctx)
    cmd = '{}/bin/daliadmin server={} workunit {}'.format(get_system_dir(ctx), eclagent_host, wuid)
    # can be inefficient if the output is very large
    output = execute(cmd, silent=True, capture=True)
    doc = etree.fromstring(output)
    participatant_roxie_nodes = set()
    for record in doc.xpath('//Statistic[@creator]'):
        if 'myroxie@' in record.attrib['creator']:
            participatant_roxie_nodes.add(str(record.attrib['creator']).split('@')[-1])
    print(participatant_roxie_nodes)


@cli.command()
@click.pass_context
def clean_unused_files(ctx):
    eclagent_host = get_roxie(ctx)
    cmd = '{}/bin/ecl roxie unused-files myroxie --server={} --delete'.format(get_system_dir(ctx), eclagent_host)
    execute(cmd)
