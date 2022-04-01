import boto3
import fasts3
import fsspec
import io
import multiprocessing as mp
import time


# Constants used in testing.
ENDPOINT_URL='http://10.62.64.207'
BUCKET="joshuarobinson"

# Small object download and group of object download
SMALL_OBJECT="2021-06-04-17.03.28.jpg"
SMALL_PATH=BUCKET + '/' + SMALL_OBJECT
GROUPPATH="balloons/"

# for list_objects_v2 and ls
LISTPREFIX="opensky/staging1/movements/"
LISTPATH=BUCKET + '/' + LISTPREFIX

# Large objects of various sizes
TEST_OBJECTS = ["foo64m.txt", "foo128m.txt", "foo256m.txt", "foo500m.txt", "foo1g.txt", "foo2g.txt", "foo3g.txt", "foo4g.txt", "foo.txt", "foo10G.txt"]


# Initialize boto3, fsspec, and fasts3
storage_options = {'endpoint_url': ENDPOINT_URL}
fs = fsspec.filesystem('s3', client_kwargs=storage_options)

s = fasts3.FastS3FileSystem(endpoint=ENDPOINT_URL)

s3r = boto3.resource('s3', endpoint_url=ENDPOINT_URL)
bucket = s3r.Bucket(BUCKET)


print("Benchmarking small get_object operation")
start = time.time()
contents = s.get_objects([SMALL_PATH])[0]
elapsed_rust = time.time() - start
print("fasts3 small get_object, len={}, {}".format(len(contents), elapsed_rust))

start = time.time()
bytes_buffer = io.BytesIO()
s3r.meta.client.download_fileobj(Bucket=BUCKET, Key=SMALL_OBJECT, Fileobj=bytes_buffer)
elapsed_b = time.time() - start
print("boto3 small download, len={}, {}".format(bytes_buffer.getbuffer().nbytes, elapsed_b))

start = time.time()
data = bucket.Object(SMALL_OBJECT).get().get('Body').read()
elapsed_bg = time.time() - start
print("boto3 small get, len={}, {}".format(len(data), elapsed_bg))

if bytes_buffer.getbuffer() != contents:
    print("Error, mismatched contents")
    exit()

print("Rust is {:.1f}x faster than Python download_fileobj and {:.1f}x faster than Python get".format(elapsed_b / elapsed_rust, elapsed_bg / elapsed_rust))


print("Benchmarking group get_object operation")

IMGPATH=BUCKET + "/" + GROUPPATH
image_keys = s.ls(IMGPATH)
image_paths = [BUCKET + '/' + p for p in image_keys]

s.get_objects(image_paths)  # Cache warming

start = time.time()
contents = s.get_objects(image_paths)
elapsed_rust = time.time() - start
print("fasts3 download group, len={}, {}".format(len(contents), elapsed_rust))

# Define boto3 download function so it can be used by Threadpool map()
def do_boto3_download(key: str):
    bytes_buffer = io.BytesIO()
    s3r.meta.client.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=bytes_buffer)
    return bytes_buffer.getbuffer()

pool = mp.pool.ThreadPool()
start = time.time()
pycontents = pool.map(do_boto3_download, image_keys)
elapsed_b = time.time() - start
print("boto3 download group, len={}, {}".format(len(pycontents), elapsed_b))

if len(pycontents) != len(contents):
    print("ERROR, wrong number of files returned")
    exit()

for a,b in zip(pycontents, contents):
    if a != b:
        print("ERROR, mismatched contents!")
        exit()

print("Rust is {:.1f}x faster than Python".format(elapsed_b / elapsed_rust))


print("Benchmarking large get_object operation")

for _ in range(3):
    for object_key in TEST_OBJECTS:
        object_path = BUCKET + '/' + object_key
        print(object_path)

        start = time.time()
        contents = s.get_objects([object_path])[0]
        elapsed_rust = time.time() - start
        print("fasts3 large get_object, len={}, {}".format(len(contents), elapsed_rust))

        start = time.time()
        with fs.open('/' + object_path) as fp:
            foo = fp.read()
        elapsed_fs = time.time() - start
        print("Download-fsspec ", elapsed_fs)

        start = time.time()
        bytes_buffer = io.BytesIO()
        s3r.meta.client.download_fileobj(Bucket=BUCKET, Key=object_key, Fileobj=bytes_buffer)
        elapsed_b = time.time() - start
        print("boto3 large download, len={}, {}".format(bytes_buffer.getbuffer().nbytes, elapsed_b))

        start = time.time()
        data = bucket.Object(object_key).get().get('Body').read()
        elapsed_bg = time.time() - start
        print("boto3 large get, len={}, {}".format(len(data), elapsed_bg))

        if bytes_buffer.getbuffer() != contents:
            print("ERROR, mismatched contents")
            exit()

        object_size_mb = len(contents) / (1024 * 1024)
        print("Rust is {:.1f}x faster than Python download_fileobj and {:.1f}x faster than Python get".format(elapsed_b / elapsed_rust, elapsed_bg / elapsed_rust))
        # Print in a manner that can be easily converted to CSV for plotting
        print("RESULT-get,{:.1f},{},{},{},{}".format(object_size_mb, elapsed_fs, elapsed_bg, elapsed_b, elapsed_rust))



print("Benchmarking list operation")
start = time.time()
listingc = fs.ls(LISTPATH)
elapsed_fs = time.time() - start
print("fsspec-s3 ls, len={}, {}".format(len(listingc), elapsed_fs))

start = time.time()
listing = s.ls(LISTPATH)
elapsed_rust = time.time() - start
print("fasts3 ls, len={}, {}".format(len(listing), elapsed_rust))

start = time.time()
paginator = s3r.meta.client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix=LISTPREFIX)
# The line below assumes at least one key returned.
listingb = [obj['Key'] for page in pages for obj in page['Contents']]
elapsed_py = time.time() - start
print("boto3 ls, len={}, {}".format(len(listingb), elapsed_py))
    
print("Rust ls() is {:.1f}x faster than Boto3 and {:.1f}x faster than fsspec".format(elapsed_py / elapsed_rust, elapsed_fs / elapsed_rust))
print("RESULT-ls,{},{},{},{}".format(len(listing), elapsed_fs, elapsed_py, elapsed_rust))
