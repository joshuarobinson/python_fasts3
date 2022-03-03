import boto3
import fasts3
import fsspec
import io
import time


ENDPOINT_URL='http://10.62.64.207'
BUCKET="joshuarobinson"
OBJECT="foo.txt"

SMALL_OBJECT="2021-06-04-17.03.28.jpg"

# Initialize boto3, fsspec, and fasts3
storage_options = {'endpoint_url': ENDPOINT_URL}
fs = fsspec.filesystem('s3', client_kwargs=storage_options)

s = fasts3.FastS3FileSystem(endpoint=ENDPOINT_URL)

s3r = boto3.resource('s3', endpoint_url=ENDPOINT_URL)


print("Benchmarking small get_object operation")
start = time.time()
bytes_buffer = io.BytesIO()
s3r.meta.client.download_fileobj(Bucket=BUCKET, Key=SMALL_OBJECT, Fileobj=bytes_buffer)
elapsed_b = time.time() - start
print("boto3 download small, len={}, {}".format(bytes_buffer.getbuffer().nbytes, elapsed_b))

start = time.time()
contents = s.get_object(BUCKET, SMALL_OBJECT)
elapsed_rust = time.time() - start
print("fasts3 get_object small, len={}, {}".format(len(contents), elapsed_rust))

if bytes_buffer.getbuffer() != contents:
    print("Error, mismatched contents")

print("Rust is {:.1f}x faster than Python".format(elapsed_b / elapsed_rust))


print("Benchmarking large get_object operation")
start = time.time()
bytes_buffer = io.BytesIO()
s3r.meta.client.download_fileobj(Bucket=BUCKET, Key=OBJECT, Fileobj=bytes_buffer)
elapsed_b = time.time() - start
print("boto3 download, len={}, {}".format(bytes_buffer.getbuffer().nbytes, elapsed_b))

start = time.time()
contents = s.get_object(BUCKET, OBJECT)
elapsed_rust = time.time() - start
print("fasts3 get_object, len={}, {}".format(len(contents), elapsed_rust))

if bytes_buffer.getbuffer() != contents:
    print("Error, mismatched contents")

print("Rust is {:.1f}x faster than Python".format(elapsed_b / elapsed_rust))


print("Benchmarking list operation")
start = time.time()
listingc = fs.ls('/joshuarobinson/opensky/staging1/movements/')
elapsed_fs = time.time() - start
print("fsspec-s3 ls, len={}, {}".format(len(listingc), elapsed_fs))


start = time.time()
listing = s.ls('joshuarobinson/opensky/staging1/movements/')
elapsed_rust = time.time() - start
print("fasts3 ls, len={}, {}".format(len(listing), elapsed_rust))

start = time.time()
paginator = s3r.meta.client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket='joshuarobinson', Prefix='opensky/staging1/movements/')
listingb = [obj['Key'] for page in pages for obj in page['Contents']]
elapsed_py = time.time() - start
print("boto3 ls, len={}, {}".format(len(listingb), elapsed_py))
    
print("Rust is {:.1f}x faster than Boto3 and {:.1f}x faster than fsspec".format(elapsed_py / elapsed_rust, elapsed_fs / elapsed_rust))
