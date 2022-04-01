#!/bin/bash

IMG="pyo3-test"

docker run -it --rm  --name fasts3-bench \
    --env-file ./credentials \
    $IMG \
    python3 /benchmark.py
