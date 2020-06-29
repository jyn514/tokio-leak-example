use tokio::runtime::Runtime;

use rusoto_core::region::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

use failure::Error;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

fn main() {
    for _ in 0..50 {
        let mut backend = S3Backend {
            client: S3Client::new(Region::UsWest1),
            bucket: "rust-docs-rs".into(),
            runtime: Runtime::new().unwrap(),
        };
        let uploads = vec![Blob {
            path: "/path/to/file".into(),
            mime: "application/json".into(),
            content: br#"{"a": "b"}"#.to_vec(),
        }];
        store_batch(&mut backend, uploads).unwrap();
    }
}

struct S3Backend {
    client: S3Client,
    bucket: String,
    runtime: Runtime,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Blob {
    path: String,
    mime: String,
    content: Vec<u8>,
}

fn store_batch(backend: &mut S3Backend, mut uploads: Vec<Blob>) -> Result<(), Error> {
        // `FuturesUnordered` is used because the order of execution doesn't
        // matter, we just want things to execute as fast as possible
        let mut futures = FuturesUnordered::new();

        // Drain uploads, filling `futures` with upload requests
        for blob in uploads.drain(..) {
            futures.push(
                backend.client
                    .put_object(PutObjectRequest {
                        bucket: backend.bucket.to_string(),
                        key: blob.path.clone(),
                        body: Some(blob.content.clone().into()),
                        content_type: Some(blob.mime.clone()),
                        ..Default::default()
                    })
            );

            // Try to upload each blob
            for _ in backend.runtime.handle().block_on(futures.next()) {}
        }

        Ok(())
    }

