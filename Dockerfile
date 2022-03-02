FROM python:3.8-buster

RUN python3 -m pip install boto3 fsspec s3fs

ADD fasts3/target/wheels/ /

RUN python3 -m pip install /*.whl

ADD *.py /
