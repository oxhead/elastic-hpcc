import os
import logging

import click
from executor import execute

from elastic import init
from elastic.util import parallel
from elastic.benchmark.workload import Workload, WorkloadExecutionTimeline
from elastic.benchmark.zeromqimpl import *
from elastic.benchmark.roxie import RoxieBenchmark
from elastic.util import network as network_util
from elastic.benchmark.service import BenchmarkService
from elastic.hpcc.base import HPCCCluster


# TODO: temporary solution
init.setup_logging(config_path="conf/logging.yaml", log_dir="logs", component="benchmark")
logger = logging.getLogger(__name__)


@click.group()
@click.option('--config', type=click.Path(exists=True, resolve_path=True), default="conf/benchmark.yaml")
@click.pass_context
def cli(ctx, **kwargs):
    """This is a command line tool that works for VCL instances
    """
    ctx.obj = kwargs
    if ctx.obj['config'] is not None:
        ctx.obj['_config'] = BenchmarkConfig.parse_file(ctx.obj['config'])

@cli.command()
@click.option('--dataset', multiple=True, default=['anagram2', 'originalperson', 'sixdegree'])
@click.pass_context
def download_dataset(ctx, dataset):
    print(dataset)
    dataset_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..",  "dataset")
    print(dataset_dir)
    if 'anagram2' in dataset:
        anagram2_dataset_dir = os.path.join(dataset_dir, "Anagram2")
        execute("mkdir -p {}".format(anagram2_dataset_dir))
        execute("wget http://downloads.sourceforge.net/wordlist/12dicts-5.0.zip -O {}/12dicts-5.0.zip".format(anagram2_dataset_dir))
        execute("unzip {}/12dicts-5.0.zip -d {}".format(anagram2_dataset_dir, anagram2_dataset_dir))
    if 'originalperson' in dataset:
        originalperson_dataset_dir = os.path.join(dataset_dir, "OriginalPerson")
        execute("mkdir -p {}".format(originalperson_dataset_dir))
        execute("wget http://wpc.423a.rhocdn.net/00423A/install/docs/3_8_0_8rc_CE/OriginalPerson -O {}/OriginalPerson".format(originalperson_dataset_dir))
    if 'sixdegree' in dataset:
        sixdegree_dataset_dir = os.path.join(dataset_dir, "SixDegree")
        execute("mkdir -p {}".format(sixdegree_dataset_dir))
        execute("wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actors.list.gz -O {}/actors.list.gz".format(sixdegree_dataset_dir))
        execute("wget ftp://ftp.fu-berlin.de/pub/misc/movies/database/actresses.list.gz -O {}/actresses.list.gz".format(sixdegree_dataset_dir)) 
        execute("zcat {}/actors.list.gz > {}/actors.list".format(sixdegree_dataset_dir, sixdegree_dataset_dir))
        execute("zcat {}/actresses.list.gz > {}/actresses.list".format(sixdegree_dataset_dir, sixdegree_dataset_dir))


@cli.command()
@click.argument('action', type=click.Choice(['start', 'stop', 'status']))
@click.pass_context
def service(ctx, action):
    benchmark_service = BenchmarkService.new(ctx.obj['config'])
    if action == "start":
        benchmark_service.start()
    elif action == "stop":
        benchmark_service.stop()
    elif action == "status":
        print(benchmark_service.status())


@cli.command()
@click.option('-n', '--node', multiple=True)
@click.pass_context
def install_package(ctx, node):
    deploy_set = set()
    if not network_util.is_local_ip(ctx.obj['_config'].get_controller()):
        deploy_set.add(ctx.obj['_config'].get_controller())
    for driver_node in ctx.obj['_config'].get_drivers():
        if not network_util.is_local_ip(driver_node):
            deploy_set.add(driver_node)
    with parallel.CommandAgent(concurrency=len(deploy_set), show_result=False) as agent:
        for host in deploy_set:
            agent.submit_remote_command(host, "cd ~/elastic-hpcc; source init.sh", silent=True)


@cli.command()
@click.pass_context
def deploy(ctx):
    deploy_set = set()
    if not network_util.is_local_ip(ctx.obj['_config'].get_controller()):
        deploy_set.add(ctx.obj['_config'].get_controller())
    for driver_node in ctx.obj['_config'].get_drivers():
        if not network_util.is_local_ip(driver_node):
            deploy_set.add(driver_node)
    with parallel.CommandAgent(concurrency=len(deploy_set), show_result=False) as agent:
        for host in deploy_set:
             # TODO: a better way? Should not use fixed directory
             agent.submit_command('rsync -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" -avz --exclude elastic-hpcc/HPCC-Platform --exclude elastic-hpcc/benchmark --exclude elastic-hpcc/results --exclude elastic-hpcc/.git --exclude elastic-hpcc/.venv ~/elastic-hpcc {}:~/'.format(host))

    with parallel.CommandAgent(concurrency=len(deploy_set), show_result=False) as agent:
        for host in deploy_set:
             agent.submit_command('rsync -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" ~/elastic-hpcc/benchmark/*.py {}:~/elastic-hpcc/benchmark'.format(host))

@cli.command()
@click.pass_context
def deploy_config(ctx):
    execute('cp {} ~/elastic-hpcc/conf/benchmark.yaml'.format(ctx.obj['config']))
    deploy_set = set()
    if not network_util.is_local_ip(ctx.obj['_config'].get_controller()):
        deploy_set.add(ctx.obj['_config'].get_controller())
    for driver_node in ctx.obj['_config'].get_drivers():
        if not network_util.is_local_ip(driver_node):
            deploy_set.add(driver_node)
    with parallel.CommandAgent(concurrency=len(deploy_set), show_result=False) as agent:
        for host in deploy_set:
            agent.submit_command("scp ~/elastic-hpcc/conf/benchmark.yaml {}:~/elastic-hpcc/conf".format(host))
    

@cli.command()
@click.argument('action', type=click.Choice(["status", "report", "statistics"]))
@click.argument('wid', type=str)
@click.pass_context
def workload(ctx, action, wid):
    commander = BenchmarkCommander(ctx.obj['_config'].get_controller(), ctx.obj['_config'].lookup_config(BenchmarkConfig.CONTROLLER_COMMANDER_PORT))
    if action == "status":
        print(commander.workload_status(wid))
    elif action == "report":
        print(commander.workload_report(wid))
    elif action == "statistics":
        print(commander.workload_statistics(wid))

@cli.command()
@click.argument('config', type=click.Path(exists=True, resolve_path=True))
@click.option('-o', '--output_dir', type=click.Path(exists=True, resolve_path=True), default="results")
@click.pass_context
def submit(ctx, config, output_dir):
    w = Workload.parse_config(config)
    workload_timeline = WorkloadExecutionTimeline.from_workload(w)
    benchmark_service = BenchmarkService.new(ctx.obj['config'])
    benchmark_service.submit_workload(workload_timeline)

@cli.command()
@click.argument('config', type=click.Path(exists=True, resolve_path=True))
@click.option('-o', '--output_dir', type=click.Path(exists=False, resolve_path=True), default="results")
@click.pass_context
def run(ctx, config, output_dir):
    hpcc_cluster = HPCCCluster.parse_config("/etc/HPCCSystems/source/hpcc_t5_r5_cyclic.xml")
    benchmark_config = BenchmarkConfig.parse_file(ctx.obj['config'])

    w = Workload.parse_config(config)
    workload_timeline = WorkloadExecutionTimeline.from_workload(w)
    bm = RoxieBenchmark(hpcc_cluster, benchmark_config, workload_timeline, output_dir=output_dir)
    bm.run()

