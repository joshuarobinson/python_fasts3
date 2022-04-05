import boto3
import fasts3
import random

ENDPOINT_URL='http://local-minio:9000'
BUCKET="testbucket"

s3r = boto3.resource('s3', endpoint_url=ENDPOINT_URL)

s3r.create_bucket(Bucket=BUCKET)
    
random.seed(0)
BUFFERSIZE = 2 * 1024 * 1024
large_buffer = bytearray(random.getrandbits(8) for _ in range(BUFFERSIZE * 256 + 67891))

src_buffers = [bytearray(random.getrandbits(8) for _ in range(BUFFERSIZE)) for _ in range(10)]

for i in range(len(src_buffers)):
    obj = s3r.Object(BUCKET, "test-" + str(i))
    obj.put(Body=src_buffers[i])

paginator = s3r.meta.client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix='')
listingb = [obj['Key'] for page in pages for obj in page['Contents']]
print(listingb)

if len(listingb) != len(src_buffers):
    print("FAIL")
    exit()

s = fasts3.FastS3FileSystem(endpoint=ENDPOINT_URL)
contents = s.get_objects([BUCKET + '/' + "test-" + str(i) for i in range(len(src_buffers))])

print(len(contents))
if len(contents) != len(src_buffers):
    print("FAIL")
    exit()

for i in range(len(src_buffers)):
    if contents[i] != src_buffers[i]:
        print("FAIL")
    
obj = s3r.Object(BUCKET, "test-big")
obj.put(Body=large_buffer)

contents = s.get_objects([BUCKET + '/' + "test-big"])[0]
if contents != large_buffer:
    print("FAIL")
    exit()
