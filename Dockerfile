FROM python:3.8-buster AS builder

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup update
RUN python3 -m pip install maturin

COPY fasts3/ fasts3/
RUN cd fasts3 && cargo fmt && maturin build --release
RUN ls fasts3/target/wheels/

#===========
FROM python:3.8-buster

RUN python3 -m pip install boto3 fsspec s3fs

COPY --from=builder fasts3/target/wheels/ /

RUN python3 -m pip install /fasts3-*-cp38-*2_28*.whl

ADD *.py /
