#!/bin/bash
export LANG=en_US.utf-8
export SHELL=/bin/bash
jupyter notebook --port 80 --ip 0.0.0.0 --no-browser </dev/null >>jupyter.out 2>&1 &

