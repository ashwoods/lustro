import click
import click_log
import os

from .db import Mirror
from .utils import oracle_qualified_dsn

os.environ['NLS_LANG'] = 'GERMAN.AL32UTF8'


@click.group()
@click_log.simple_verbosity_option()
@click_log.init(__name__)
@click.option('--source', help="Source DB DSN.")
@click.option('--target', help="Target DB DSN.")
@click.option('--tables', default=None, help="Comma separated table names")
@click.option('--views', default=None, help="Comma separated view names")
@click.option('--source-schema', default=None, help="Source schema/owner")
@click.option()
@click.pass_context
def cli(ctx, source, target, tables, views, source_schema):
    if ctx.obj is None:
        ctx.obj = {}
    if source.startswith('oracle'):
        source = oracle_qualified_dsn(source)
    if tables:
        tables = tables.split(',')
    if views:
        views = views.split(',')
    else:
        tables = ''
    ctx.obj['MIRROR'] = Mirror(source=source, target=target, source_schema=source_schema)
    ctx.obj['TABLES'] = tables


@cli.command()
@click.pass_context
def create(ctx):
    """Creates the schema but doesn't copy any data"""
    mirror = ctx.obj['MIRROR']
    mirror.create(tables=ctx.obj['TABLES'])


@cli.command()
@click.option('--field', default='modified', help='Specify name of modified field')
@click.option('--modified', default=None, help='Specify a datetime for diff')
@click.pass_context
def diff(ctx, modified, field):
    """Creates the schema if it doesn't exist and copies """
    mirror = ctx.obj['MIRROR']
    created, modified = mirror.diff(tables=ctx.obj['TABLES'], modified=modified)
    click.echo("Results %s created, %s modfied" % (created, modified))


@cli.command()
@click.option('--field', default='modified', help='Specify name of modified field')
@click.option('--modified', default=None, help='Specify a datetime for diff')
@click.pass_context
def diff_views(ctx, modified, field):
    """Creates the schema if it doesn't exist and copies """
    mirror = ctx.obj['MIRROR']
    created, modified = mirror.diff_views(tables=ctx.obj['TABLES'], modified=modified)
    click.echo("Results %s created, %s modfied" % (created, modified))

@cli.command()
@click.pass_context
def recreate(ctx):
    mirror = ctx.obj['MIRROR']
    mirror.recreate(tables=ctx.obj['TABLES'])


@cli.command()
@click.pass_context
def mirror(ctx):
    mirror = ctx.obj['MIRROR']
    mirror.mirror(tables=ctx.obj['TABLES'])
