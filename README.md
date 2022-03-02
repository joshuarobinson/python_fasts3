# python_fasts3
Fast S3 in Python using Rust

A Rust library that can be called from Python to perform S3 operations. The goal is to be significantly faster than Python-only S3 code like boto3. Currently only supports very basic ls() and get_object() functionality and is meant as a POC, not for production usage.

Using fasts3 from python should be simple and fast:
```
s = fasts3.FastS3FileSystem(endpoint=ENDPOINT_URL)

contents = s.get_object(BUCKETNAME, OBJECTNAME)
```

Example output from benchmark program:
```
Benchmarking get_object operation
...
Rust is 2.2x faster than Python
Benchmarking list operation
...
Rust is 1.8x faster than Boto3 and 1.8x faster than fsspec
```
