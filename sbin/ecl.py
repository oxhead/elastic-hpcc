import os

import click
import executor
from executor import execute

from cli import cli
from cli import get_esp
from cli import get_roxie
from cli import get_system_dir

def convert_to_query_xml(query_list):
    if query_list is None or len(query_list) == 0:
        return ""
    query_xml = ""
    for (k, v) in query_list:
        query_xml = query_xml + "<{}>{}</{}>".format(k, v, k)
    query_xml = '--input="<request>{}</request>"'.format(query_xml)
    return query_xml

@cli.command()
@click.option('--target', default='thor', type=click.Choice(['thor', 'roxie']))
@click.option('--ecl', type=click.Path(exists=True, resolve_path=True))
@click.option('--dir', '-d', type=click.Path(exists=True, resolve_path=True))
@click.option('--query', '-q', multiple=True, type=(str, str))
@click.option('--wait', type=int, default=30000)
@click.pass_context
def run(ctx, target, ecl, dir, query, wait):
    click.echo('submitting a job to {}'.format(target))
    eclagent_host = get_esp(ctx)
    program_dir = os.path.dirname(ecl) if dir is None else dir
    cmd = 'cd {}; {}/bin/ecl run --server {} --target {} --wait={} {} {}'.format(program_dir, get_system_dir(ctx), eclagent_host, target, wait, convert_to_query_xml(query), ecl)
    execute(cmd)
