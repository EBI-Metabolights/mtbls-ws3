import sys

import click

from mtbls import __version__
from mtbls.run.cli.es.index_studies import run_es_cli
from mtbls.run.cli.validation.validate_studies import run_validation_cli


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__)
def mtbls_tool():
    """MeteboLights CLI tool with subcommands."""
    pass


mtbls_tool.add_command(run_es_cli)
mtbls_tool.add_command(run_validation_cli)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        mtbls_tool(["--help"])
    else:
        mtbls_tool()
