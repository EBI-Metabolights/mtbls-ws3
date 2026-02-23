import sys

import click

from mtbls import __version__
from mtbls.run.cli.index.index_group import index_group
from mtbls.run.cli.validation.validation_group import validation_group


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__)
def mtbls_cli():
    """MetaboLights CLI tool with subcommands."""
    pass


mtbls_cli.add_command(index_group)
mtbls_cli.add_command(validation_group)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        mtbls_cli(["--help"])
    else:
        mtbls_cli()
