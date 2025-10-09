# MetaboLights v3

You can find technical documentation on (this website)[https://ebi-metabolights.github.io/mtbls-ws3/]

## Development Environment

Development environment for linux or mac

```bash

# install python package manager uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# add $HOME/.local/bin to your PATH
export PATH=$HOME/.local/bin:$PATH

# RESTART your terminal or OPEN NEW one

# install git from https://git-scm.com/downloads
# Linux command
apt update; apt install git -y

# Mac command
# brew install git

# clone project from github
git clone https://github.com/EBI-Metabolights/mtbls-ws3

cd mtbls-ws3

# install python using uv tool.
uv python install 3.13

# install python dependencies
uv sync

# install pre-commit to check repository integrity and format checking
uv run pre-commit

# install pre-commit to check repository integrity and format checking
uv run pytest

# open your IDE (vscode, pycharm, etc.) and set python interpreter as .venv/bin/python

```
