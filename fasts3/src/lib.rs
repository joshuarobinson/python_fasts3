use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyByteArray;

use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint, Error, Region};
use futures::future::try_join_all;
use http::Uri;
use tokio_stream::StreamExt;

const READCHUNK: usize = 1024 * 1024 * 128;

#[pyclass]
pub struct FastS3FileSystem {
    #[pyo3(get, set)]
    pub endpoint: String,
}

impl FastS3FileSystem {
    fn get_client(&self) -> aws_sdk_s3::Client {
        let region = Region::new("us-west-2");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let conf = rt.block_on(async { aws_config::load_from_env().await });

        let ep = Endpoint::immutable(self.endpoint.parse::<Uri>().unwrap());
        let s3_conf = aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_resolver(ep)
            .region(region)
            .build();

        Client::from_conf(s3_conf)
    }
}

// Extract path into bucket + prefix
fn path_to_bucketprefix(path: &str) -> (String, String) {
    let s3path = std::path::Path::new(path);
    let mut path_it = s3path.iter();
    let bucket = path_it.next().unwrap().to_str().unwrap();
    let mut prefix_path = std::path::PathBuf::new();
    for p in path_it {
        prefix_path.push(p);
    }
    let mut prefix = prefix_path.to_str().unwrap().to_string();
    if path.ends_with('/') {
        prefix.push('/');
    }

    (bucket.to_string(), prefix)
}

// Write contents of ByteStream into destination buffer.
async fn drain_stream(mut s: ByteStream, dest: &mut [u8]) -> Result<usize, Error> {
    let mut offset = 0;
    while let Ok(Some(bytes)) = s.try_next().await {
        let span = offset..offset + bytes.len();
        dest[span].clone_from_slice(&bytes);
        offset += bytes.len();
    }
    Ok(offset)
}

#[pymethods]
impl FastS3FileSystem {
    #[new]
    pub fn new(endpoint: String) -> FastS3FileSystem {
        FastS3FileSystem { endpoint }
    }

    pub fn ls(&self, path: &str) -> PyResult<Vec<String>> {
        let (bucket, prefix) = path_to_bucketprefix(path);

        let client = self.get_client();
        let mut continuation_token = String::from("");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let listing = rt.block_on(async {
            let mut listing: Vec<String> = Vec::new();
            loop {
                let resp = match client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .delimiter('/')
                    .continuation_token(continuation_token)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return Err(PyIOError::new_err(e.to_string())),
                };

                for object in resp.contents().unwrap_or_default() {
                    let key = object.key().unwrap_or_default();
                    listing.push(key.to_string());
                }

                if resp.is_truncated() {
                    continuation_token = resp.next_continuation_token().unwrap().to_string();
                } else {
                    break;
                }
            }
            Ok(listing)
        });
        listing
    }

    pub fn get_object(&self, py: Python, bucket: &str, key: &str) -> PyResult<PyObject> {
        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let pybuf = PyByteArray::new(py, &[]);

        let buf = rt.block_on(async {
            let resp = match client.head_object().bucket(bucket).key(key).send().await {
                Ok(r) => r,
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            };
            let obj_size = resp.content_length() as usize;
            pybuf.resize(obj_size)?;

            let landing_buf = unsafe { pybuf.as_bytes_mut() };
            let mut landing_slices: Vec<&mut [u8]> = landing_buf.chunks_mut(READCHUNK).collect();

            let mut read_offset = 0;
            let mut read_reqs = vec![];
            while read_offset < obj_size {
                let read_upper = std::cmp::min(obj_size, read_offset + READCHUNK);
                let byte_range = format!("bytes={}-{}", read_offset, read_upper - 1);

                let resp = match client
                    .get_object()
                    .bucket(bucket)
                    .key(key)
                    .range(byte_range)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => return Err(PyIOError::new_err(e.to_string())),
                };

                read_reqs.push(drain_stream(resp.body, landing_slices.remove(0)));

                read_offset += READCHUNK;
            }
            let _results = try_join_all(read_reqs).await.unwrap();

            Ok(pybuf)
        });

        match buf {
            Ok(b) => Ok(b.into()),
            Err(e) => Err(e),
        }
    }
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn fasts3(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FastS3FileSystem>()?;

    Ok(())
}
