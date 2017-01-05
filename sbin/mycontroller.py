import os
import logging

import click

from elastic import init
from elastic.benchmark.impl.zeromqimpl import *
from elastic.util import daemon


class BenchmarkControllerDaemon(daemon.Daemon):
    def __init__(self, config_path):
        super().__init__('/tmp/mycontroller.pid')
        init.setup_logging(default_level=logging.DEBUG, config_path="conf/logging.yaml", log_dir="logs", component="controller")
        self.benchmark_controller = BenchmarkController(config_path)

    def run(self):
        self.benchmark_controller.start()


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
@click.pass_context
def start(ctx):
    BenchmarkControllerDaemon(ctx.obj['config']).start()


@cli.command()
@click.pass_context
def stop(ctx):
    BenchmarkControllerDaemon(ctx.obj['config']).stop()


@cli.command()
@click.pass_context
def restart(ctx):
    BenchmarkControllerDaemon(ctx.obj['config']).restart()
