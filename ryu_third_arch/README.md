

## Installation on Debian 9

This package requires Python 3.6.

    sudo apt install virtualenv

    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda

    virtualenv /opt/amn/venv --python /opt/miniconda/bin/python3

    . /opt/amn/venv/bin/activate

## Packaging

    ./setup.py sdist bdist_wheel

    # Transfer dist/*

    . /opt/amn/venv/bin/activate
    pip install --upgrade <...>.tar.gz

