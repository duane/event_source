use super::super::commit::{Commit, CommitAttempt};
use super::{ClientStore, StoreError, StoreErrorType};
use uuid::Uuid;
use std::error::Error;
use std::fmt;
use hyper::{Request, body::Body, Uri, client::Client};
use hyper::client::HttpConnector;
use std::convert::TryFrom;
use serde::{Serialize,Deserialize}

/// The store representing a sqlite connection.
pub struct HttpStore {
  client: Client<HttpConnector, Body>
}

/// The errors returned by this store.
#[derive(Debug)]
pub struct HttpStoreError {
  // cause: RusqliteError
}

impl fmt::Display for HttpStoreError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "HttpStoreError({:})", self.error_type())
    // write!(f, "HttpStoreError({:}, {:})", self.error_type(), self.cause)
  }
}

impl Error for HttpStoreError {
  fn source(&self) -> Option<&(dyn Error + 'static)> {
    None
    // Some(&self.cause)
  }
}

impl Into<Box<dyn StoreError>> for HttpStoreError {
  fn into(self) -> Box<dyn StoreError> {
    Box::new(self)
  }
}

async fn issue_get(client: &mut Client<HttpConnector, Body>, uri: Uri) -> Result<String, hyper::Error> {
  let res = client.get(uri).await?;
  let buf = hyper::body::to_bytes(res).await?;
  let body = String::from_utf8(buf.into_iter().collect()).unwrap();

  Ok(body)
}

async fn issue_post(client: &mut Client<HttpConnector, Body>, uri: Uri, body: serde_json::Value) -> Result<String, hyper::Error> {
  let req = Request::builder()
    .method("POST")
    .uri(uri)
    .body(Body::from(body.to_string())).unwrap();
  let res = client.request(req).await?;

  let buf = hyper::body::to_bytes(res).await?;
  let body = String::from_utf8(buf.into_iter().collect()).unwrap();

  Ok(body)
}

impl StoreError for HttpStoreError {
  fn error_type(&self) -> StoreErrorType {
    StoreErrorType::UnknownError
  }
}

impl ClientStore for HttpStore {
  fn commit(&mut self, commit_attempt: &CommitAttempt) -> Result<i64, Box<dyn StoreError>> {

    todo!()
  }

  fn get_aggregate_range(
      &mut self,
      aggregate_id: Uuid,
      min_version: i64,
      max_version: i64,
  ) -> Result<Vec<Commit>, Box<dyn StoreError>> {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let response_body = rt.block_on(
      issue_get(
        &mut self.client,
        Uri::try_from(
          format!(
            "http://localhost:4321/store/{}/commits?from={}&to={}",
            aggregate_id,
            min_version,
            max_version
          )
        ).unwrap()
      )
    ).unwrap();
    Ok(serde_json::from_str(response_body.as_ref()).unwrap())
  }
}
