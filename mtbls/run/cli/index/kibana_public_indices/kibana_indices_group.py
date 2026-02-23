import sys

import click

from mtbls.run.cli.index.kibana_public_indices.maintain import maintain_cli
from mtbls.run.cli.index.kibana_public_indices.reindex_all import reindex_all_cli
from mtbls.run.cli.index.kibana_public_indices.reindex_selected import (
    reindex_selected_cli,
)


@click.group(
    name="kibana-indices", context_settings={"help_option_names": ["-h", "--help"]}
)
def kibana_indices_group():
    """MetaboLights Kibana Data Index CLI tool with subcommands."""
    pass


kibana_indices_group.add_command(maintain_cli)
kibana_indices_group.add_command(reindex_selected_cli)
kibana_indices_group.add_command(reindex_all_cli)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        kibana_indices_group(["--help"])
    else:
        kibana_indices_group()
