use pyo3::prelude::*;

mod fasts3_filesystem;
use crate::fasts3_filesystem::FastS3FileSystem;

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn fasts3(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FastS3FileSystem>()?;

    Ok(())
}
