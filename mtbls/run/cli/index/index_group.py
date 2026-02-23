import sys

import click

from mtbls.run.cli.index.kibana_public_indices.kibana_indices_group import (
    kibana_indices_group,
)


@click.group(name="indices", context_settings={"help_option_names": ["-h", "--help"]})
def index_group():
    """MetaboLights Data Index CLI tool with subcommands."""
    pass


index_group.add_command(kibana_indices_group)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        index_group(["--help"])
    else:
        index_group()
