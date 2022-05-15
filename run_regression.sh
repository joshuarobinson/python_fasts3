#!/bin/bash

IMG="pyo3-test"
MINIO_IMG=quay.io/minio/minio

ACCESSKEY="AKIAIOSFODMM7EXAMPLE"
SECRETKEY="wJalrXUtnFEMI/K7MDENG/bQxRfiCYEXAMPLEKEY"

echo "Starting local minio instance"
docker run --rm -d --name local-minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=$ACCESSKEY" \
  -e "MINIO_ROOT_PASSWORD=$SECRETKEY" \
  $MINIO_IMG server /data --console-address ":9001"

echo "Starting fasts3 test client"
docker run -it --rm  --name fasts3-test \
    --link local-minio:local-minio \
    -e "AWS_ACCESS_KEY_ID=$ACCESSKEY" \
    -e "AWS_SECRET_ACCESS_KEY=$SECRETKEY" \
    $IMG \
    python3 /regression_test.py

echo "Finished, now shutting down local minio."
docker stop local-minio
