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


def convert_to_query_text(query_list):
    if query_list is None or len(query_list) == 0:
        return ""
    query_text = " ".join(["-X{}={}".format(k, v) for k, v in query_list])
    return query_text

def convert_to_query_xml(query_list):
    if query_list is None or len(query_list) == 0:
        return ""
    query_xml = ""
    for (k, v) in query_list:
        query_xml = query_xml + "<{}>{}</{}>".format(k, v, k)
    query_xml = '--input="<request>{}</request>"'.format(query_xml)
    return query_xml

def lookup_status(ctx, name, type): 
    wuid, job_status = next(iter(ctx.invoke(status, name=name, type=type).items())) # dangerous
    return (wuid, job_status)

def extract_wuid(output, call_type):
    if call_type == 'pq':
        return output.splitlines()[1][3:]
    elif call_type == 'ecl':
        for line in output.splitlines():
            if line.startswith('Running deployed workunit'):
                return line.rstrip().split()[-1]
    return None


def _call(ctx, target_cluster, ecl, published_query, base_dir, query_input, parameter_input, wait, job, wait_until_complete):
    if wait_until_complete:
        wait = 1000 #milliseconds
    eclagent_host = get_esp(ctx)

    call_target = None
    # should have more than three: ecl, pq, wuid, archive, dl.
    call_type = 'pq' if published_query is not None else 'ecl'
    program_dir = None
    if call_type == 'pq':
        call_target = published_query
    elif call_type == 'ecl':
        program_dir = os.path.dirname(ecl) if base_dir is None else base_dir
        call_target = ecl

    job_name = "{}_{}".format(os.path.splitext(os.path.basename(call_target))[0].lower(), uuid.uuid4()) if job is None else job
    # -v for getting wuid
    cmd = '{}/bin/ecl run -v --server {} --target {} --wait={} --name={} {} {} {}'.format(get_system_dir(ctx), eclagent_host, target_cluster, wait, job_name, convert_to_query_xml(query_input), convert_to_query_text(parameter_input), call_target)
    if program_dir is not None:
        cmd = 'cd {}; '.format(program_dir) + cmd
    print(cmd)
    output = execute(cmd, silent=True, capture=True)
    if ctx.obj['show']:
        print(output)
    #print(output)
    # this can be dangerous and inefficient
    wuid = extract_wuid(output, call_type=call_type)

    if not wait_until_complete:
        return lookup_status(ctx, wuid, type='wuid')

    return ctx.invoke(wait_for_completion, wuid=wuid)

@cli.command()
@click.argument('wuid')
@click.pass_context
def wait_for_completion(ctx, wuid):
    job_running = True
    while job_running:
        with CaptureOutput() as output:
            (wuid, job_status) = lookup_status(ctx, wuid, type='wuid')
            # completed, compiled and failed?
            #if 'ed' in job_status:
            # haven't check yet and need to confirm later?
            if 'ed' in job_status and (not 'blocked' in job_status) and (not 'submitted' in job_status):
                job_running = False
        if job_running:
            time.sleep(1)
    return lookup_status(ctx, wuid, type='wuid')


@cli.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--target', default='thor', type=click.Choice(['thor', 'roxie']))
@click.option('--ecl', type=click.Path(exists=True, resolve_path=True))
@click.option('--name', help='The name of the published query')
@click.option('--dir', '-d', type=click.Path(exists=True, resolve_path=True))
@click.option('--input', '-i', multiple=True, type=(str, str))
@click.option('--parameter', '-p', multiple=True, type=(str, str))
@click.option('--wait', type=int, default=30000)
@click.option('--job')
@click.option('--wait_until_complete', is_flag=True)
@click.pass_context
def run(ctx, target, ecl, name, dir, input, parameter, wait, job, wait_until_complete):
    return _call(ctx, target, ecl, name, dir, input, parameter, wait, job, wait_until_complete)

@cli.command()
@click.argument('name')
@click.option('--type', type=click.Choice(['job', 'wuid']))
@click.pass_context
def status(ctx, name, type):
    eclagent_host = get_esp(ctx)
    cmd_lookup = ("-n {}" if type == 'job' else '-wu {}').format(name)
    cmd = '{}/bin/ecl status --server={} {}'.format(get_system_dir(ctx), eclagent_host, cmd_lookup)
    results = execute(cmd, capture=True, silent=True)
    status_records = {}
    if ',' in results:
        status_records = {line.split(',')[0]: line.split(',')[2] for line in results.splitlines()}
    else:
        if type == 'job':
            with CaptureOutput() as output:
               wuid = ctx.invoke(lookup_wuid, job=job)[0]
               status_records[wuid] = results
        elif type == 'wuid':
            status_records[name] = results
    for (wuid, status) in status_records.items():
        print("{}: {}".format(wuid, status))
    return status_records

@cli.command()
@click.argument('job')
@click.pass_context
def lookup_wuid(ctx, job):
    eclagent_host = get_esp(ctx)
    cmd = '{}/bin/ecl getwuid --server={} -n={}'.format(get_system_dir(ctx), eclagent_host, job)
    wuid = execute(cmd, capture=True, silent=True)
    print(uuid)
    return wuid.splitlines()
