use tokio::runtime::Runtime;

use rusoto_core::region::Region;
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

use failure::Error;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

fn main() {
    for _ in 0..15 {
        let creds = DefaultCredentialsProvider::new().unwrap();
        let client = S3Client::new_with(
            rusoto_core::request::HttpClient::new().unwrap(),
            creds,
            std::env::var("S3_ENDPOINT")
                .ok()
                .map(|e| Region::Custom {
                    name: std::env::var("S3_REGION").unwrap_or_else(|_| "us-west-1".to_owned()),
                    endpoint: e,
                })
                .unwrap_or(Region::UsWest1),
        );
        let mut backend = S3Backend {
            client,
            bucket: "rust-docs-rs".into(),
            runtime: Runtime::new().unwrap(),
        };
        let uploads = vec![Blob {
            path: "file".into(),
            mime: "application/json".into(),
            content: br#"{"a": "b"}"#.to_vec(),
        }];
        let _ = store_batch(&mut backend, uploads);
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

fn store_batch(backend: &mut S3Backend, uploads: Vec<Blob>) -> Result<(), Error> {
    // `FuturesUnordered` is used because the order of execution doesn't
    // matter, we just want things to execute as fast as possible
    let mut futures = FuturesUnordered::new();

    for blob in uploads {
        futures.push(
            backend.client
                .put_object(PutObjectRequest {
                    bucket: backend.bucket.to_string(),
                    key: blob.path.clone(),
                    body: Some(blob.content.clone().into()),
                    content_type: Some(blob.mime.clone()),
                    ..Default::default()
                })
                .map(|resp| match resp {
                    Ok(..) => (),
                    Err(err) => eprintln!("error: {}", err),
                }),
        );
    }

    for _ in backend.runtime.handle().block_on(futures.next()) {}
    Ok(())
}
