import boto3
import fasts3
import fsspec
import random

ENDPOINT_URL='http://local-minio:9000'
BUCKET="testbucket"

s3r = boto3.resource('s3', endpoint_url=ENDPOINT_URL)

s3r.create_bucket(Bucket=BUCKET)

storage_options = {'endpoint_url': ENDPOINT_URL}
fs = fsspec.filesystem('s3', client_kwargs=storage_options)

s = fasts3.FastS3FileSystem(endpoint=ENDPOINT_URL)

#######

# Initialize data on the remote (test) object store
random.seed(0)
BUFFERSIZE = 2 * 1024 * 1024
large_buffer = bytearray(random.getrandbits(8) for _ in range(BUFFERSIZE * 256 + 67891))

src_buffers = [bytearray(random.getrandbits(8) for _ in range(BUFFERSIZE)) for _ in range(10)]

for i in range(len(src_buffers)):
    obj = s3r.Object(BUCKET, "test-" + str(i))
    obj.put(Body=src_buffers[i])


## BEGIN test scenarios
paginator = s3r.meta.client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix='')
listingb = [obj['Key'] for page in pages for obj in page['Contents']]
print(listingb)

assert len(listingb) == len(src_buffers), "FAIL on returned number of objects from list"

contents = s.get_objects([BUCKET + '/' + "test-" + str(i) for i in range(len(src_buffers))])

assert len(contents) == len(src_buffers), "FAIL on returning all object contents"

for i in range(len(src_buffers)):
    assert contents[i] == src_buffers[i], "FAIL, content mismatch on object {}".format(i)
    
obj = s3r.Object(BUCKET, "test-big")
obj.put(Body=large_buffer)

BIGPATH=BUCKET + "/test-big"
contents = s.get_objects([BIGPATH])[0]
assert contents == large_buffer, "FAIL on large buffer content match"

stat = fs.info(BIGPATH)

fast_stat = s.info(BIGPATH)
print(fast_stat)

# Skipping LastModified for now because of timezone support.
keys_to_check = ["Key", "ETag", "Size", "StorageClass", "VersionId"]
for k in keys_to_check:
    assert stat[k] == fast_stat[k], "FAIL on {}".format(k)
