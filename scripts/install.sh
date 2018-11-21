#!/bin/bash

echo `which python`

export PATH
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

if command -v pyenv 1>/dev/null 2>&1; then
   eval "$(pyenv init -)"
 fi

 export SLUGIFY_USES_TEXT_UNIDECODE=yes

pip install -U -r /home/ec2-user/aqueduct/requirements.txt