import os
import time
import uuid

import click
import executor
from executor import execute

from common import CaptureOutput
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
@click.option('--job')
@click.option('--wait_until_complete', is_flag=True)
@click.pass_context
def run(ctx, target, ecl, dir, query, wait, job, wait_until_complete):
    click.echo('submitting a job to {}'.format(target))
    if wait_until_complete:
        wait = 1000 #milliseconds
    eclagent_host = get_esp(ctx)
    program_dir = os.path.dirname(ecl) if dir is None else dir
    job_name = "{}_{}".format(os.path.splitext(os.path.basename(ecl))[0].lower(), uuid.uuid4()) if job is None else job
    cmd = 'cd {}; {}/bin/ecl run --server {} --target {} --wait={} --name={} {} {}'.format(program_dir, get_system_dir(ctx), eclagent_host, target, wait, job_name, convert_to_query_xml(query), ecl)
    execute(cmd)
    if not wait_until_complete:
        return
    job_running = True
    print('waiting for {} to complete'.format(job_name))
    while job_running:
        with CaptureOutput() as output:
            wuid, job_status = next(iter(ctx.invoke(status, job=job_name).items())) # dangerous
            if job_status == 'completed':
                job_running = False
        time.sleep(1)

@cli.command()
@click.argument('job')
@click.pass_context
def status(ctx, job):
    click.echo('lookup the job status')
    eclagent_host = get_esp(ctx)
    cmd = '{}/bin/ecl status --server={} -n={}'.format(get_system_dir(ctx), eclagent_host, job)
    results = execute(cmd, capture=True, silent=True)
    status_records = {}
    if ',' in results:
        status_records = {line.split(',')[0]: line.split(',')[2] for line in results.splitlines()}
    else:
        with CaptureOutput() as output:
            wuid = ctx.invoke(lookup_wuid, job=job)[0]
            status_records[wuid] = results
    for (wuid, status) in status_records.items():
        print("{}: {}".format(wuid, status))
    return status_records

@cli.command()
@click.argument('job')
@click.pass_context
def lookup_wuid(ctx, job):
    click.echo('lookup the workunit id')
    eclagent_host = get_esp(ctx)
    cmd = '{}/bin/ecl getwuid --server={} -n={}'.format(get_system_dir(ctx), eclagent_host, job)
    wuid = execute(cmd, capture=True, silent=True)
    print(uuid)
    return wuid.splitlines()
