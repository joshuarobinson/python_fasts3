use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyByteArray, PyDateTime, PyList};

use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint, Error, Region};
use futures::future::try_join_all;
use http::Uri;
use tokio_stream::StreamExt;

const READCHUNK: usize = 1024 * 1024 * 128;

#[allow(dead_code)]
fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[pyclass]
pub struct FastS3FileSystem {
    #[pyo3(get, set)]
    pub endpoint: String,

    s3_client: aws_sdk_s3::Client,
}

fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let region = Region::new("us-west-2");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let conf = rt.block_on(async { aws_config::load_from_env().await });

    let s3_conf = match endpoint.is_empty() {
        true => aws_sdk_s3::config::Builder::from(&conf).region(region).build(),
        false => aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_resolver(Endpoint::immutable(endpoint.parse::<Uri>().unwrap()))
            .region(region)
            .build(),
    };

    Client::from_conf(s3_conf)
}

impl FastS3FileSystem {
    fn get_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }
}

// Extract path into bucket + prefix
fn path_to_bucketprefix(path: &String) -> (String, String) {
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
        let c = build_client(&endpoint);
        FastS3FileSystem {
            endpoint: endpoint,
            s3_client: c,
        }
    }

    pub fn ls(&self, path: &str) -> PyResult<Vec<String>> {
        let (bucket, prefix) = path_to_bucketprefix(&path.to_string());

        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let listing = rt.block_on(async {
            let mut page_stream = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .delimiter('/')
                .into_paginator()
                .send();

            let mut listing: Vec<String> = Vec::new();
            while let Some(Ok(lp)) = page_stream.next().await {
                for object in lp.contents().unwrap_or_default() {
                    let key = object.key().unwrap_or_default();
                    listing.push(key.to_string());
                }
            }
            Ok(listing)
        });
        listing
    }

    pub fn info(&self, py: Python, path: &str) -> PyResult<PyObject> {
        let (bucket, key) = path_to_bucketprefix(&path.to_string());
        let mut key_vals: Vec<(&str, PyObject)> = vec![("Key", path.to_object(py))];

        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let _info_result = rt.block_on(async {
            let res = match client.head_object().bucket(bucket).key(key).send().await {
                Ok(r) => r,
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            };
            key_vals.push(("Size", res.content_length.to_object(py)));
            if let Some(etag) = res.e_tag {
                key_vals.push(("ETag", etag.to_object(py)));
            }
            if let Some(mtime) = res.last_modified {
                let pytime = PyDateTime::from_timestamp(py, mtime.secs() as f64, None)?;
                key_vals.push(("LastModified", pytime.to_object(py)));
            }
            if let Some(ver) = res.version_id {
                key_vals.push(("VersionId", ver.to_object(py)));
            } else {
                key_vals.push(("VersionId", py.None()));
            }
            if let Some(sc) = res.storage_class {
                key_vals.push(("StorageClass", sc.as_str().to_object(py)));
            } else {
                // Default to standard because s3fs does.
                key_vals.push(("StorageClass", "STANDARD".to_object(py)));
            }
            // Several fields not implemented, like custom metadata.

            Ok(())
        });
        let dict = key_vals.into_py_dict(py);
        Ok(dict.into())
    }

    pub fn get_objects(&self, py: Python, paths: Vec<String>) -> PyResult<PyObject> {
        let pathpairs: Vec<(String, String)> = paths.iter().map(path_to_bucketprefix).collect();

        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();

        let mut pybuf_list = Vec::new();
        for _ in &pathpairs {
            pybuf_list.push(PyByteArray::new(py, &[]));
        }

        let return_buf = rt.block_on(async {
            let mut head_reqs = vec![];
            for (bucket, key) in &pathpairs {
                head_reqs.push(client.head_object().bucket(bucket).key(key).send());
            }
            let head_results = match try_join_all(head_reqs).await {
                Ok(r) => r,
                Err(e) => return Err(PyIOError::new_err(e.to_string())),
            };
            let obj_sizes: Vec<usize> = head_results.iter().map(|x| x.content_length() as usize).collect();

            for (p, o) in pybuf_list.iter_mut().zip(obj_sizes) {
                p.resize(o)?;
            }

            let mut read_reqs = vec![];

            for (pybuf, (bucket, key)) in pybuf_list.iter_mut().zip(&pathpairs) {
                let obj_size = pybuf.len();
                let landing_buf = unsafe { pybuf.as_bytes_mut() };
                let mut landing_slices: Vec<&mut [u8]> = landing_buf.chunks_mut(READCHUNK).collect();

                let mut read_offset = 0;
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
            }
            let _results = try_join_all(read_reqs).await.unwrap();

            let pybufs: &PyList = PyList::new(py, pybuf_list);
            Ok(pybufs)
        });

        match return_buf {
            Ok(b) => Ok(b.into()),
            Err(e) => Err(e),
        }
    }
}
