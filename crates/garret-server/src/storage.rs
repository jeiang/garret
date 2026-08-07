//! S3 blob storage (MEGA S4 in production, Garage locally). Spec 03-storage.

use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_s3::{
    Client, config::Credentials, presigning::PresigningConfig, primitives::ByteStream,
};

use crate::config::S3Config;

#[derive(Clone)]
pub struct Storage {
    client: Client,
    bucket: String,
}

pub fn key_for(store_path_hash: &str) -> String {
    format!("nar/{store_path_hash}.nar.zst")
}

impl Storage {
    pub async fn new(cfg: &S3Config) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(
            aws_sdk_s3::config::Region::new(
                cfg.region.clone().unwrap_or_else(|| "us-east-1".into()),
            ),
        );
        if let (Some(id), Some(secret)) = (&cfg.access_key_id, &cfg.secret_access_key) {
            loader = loader.credentials_provider(Credentials::new(
                id.clone(),
                secret.clone(),
                None,
                None,
                "garret-config",
            ));
        }
        let mut builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        if let Some(url) = &cfg.endpoint_url {
            builder = builder.endpoint_url(url.clone());
        }
        // S4 and Garage both accept path-style; it avoids DNS games for dotted buckets.
        builder = builder.force_path_style(cfg.path_style);
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
        })
    }

    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("uploading {key}"))?;
        Ok(())
    }

    /// The Puller redirects here instead of proxying bytes (ADR-0005).
    pub async fn presigned_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(ttl)?)
            .await
            .with_context(|| format!("presigning {key}"))?;
        Ok(req.uri().to_owned())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn key_layout_is_flat_and_derivable() {
        assert_eq!(super::key_for("abc"), "nar/abc.nar.zst");
    }
}
