use tokio::runtime::Runtime;

use rusoto_core::region::Region;
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{PutObjectRequest, S3Client, S3};

use failure::Error;

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

fn main() {
    for _ in 0..50 {
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

fn store_batch(backend: &mut S3Backend, mut uploads: Vec<Blob>) -> Result<(), Error> {
    let mut attempts = 0;

    loop {
        // `FuturesUnordered` is used because the order of execution doesn't
        // matter, we just want things to execute as fast as possible
        let mut futures = FuturesUnordered::new();

        // Drain uploads, filling `futures` with upload requests
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
                    // Drop the value returned by `put_object` because we don't need it,
                    // emit an error and replace the error values with the blob that failed
                    // to upload so that we can retry failed uploads
                    .map(|resp| match resp {
                        Ok(..) => Ok(()),
                        Err(err) => {
                            eprintln!("error: {}", err);
                            Err(blob)
                        }
                    }),
            );
        }
        attempts += 1;

        // Collect all the failed uploads so that we can retry them
        while let Some(upload) = backend.runtime.handle().block_on(futures.next()) {
            if let Err(blob) = upload {
                uploads.push(blob);
            }
        }

        // If there are no further uploads we were successful and can return
        if uploads.is_empty() {
            break;

        // If more than three attempts to upload fail, return an error
        } else if attempts >= 3 {
            failure::bail!("Failed to upload to s3 three times, abandoning");
        }
    }

    Ok(())
    }

