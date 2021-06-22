use crate::commit::Commit;
use super::{StoreError, StoreErrorType};
use uuid::Uuid;
use std::error::Error;
use std::fmt;
use hyper::{Request, body::Body, Uri, client::Client};
use hyper::client::HttpConnector;
use std::convert::TryFrom;
use crate::client::event_source::EventSource;
use crate::client::command_processor::CommandProcessor;
use crate::command::Command;
use either::Either;
use crate::client::ClientError;
use crate::server::aggregate::HttpErrorResponse;

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

async fn issue_post(client: &mut Client<HttpConnector, Body>, uri: Uri, body: serde_json::Value) -> Result<(http::status::StatusCode, String), hyper::Error> {
  let req = Request::builder()
    .method("POST")
    .uri(uri)
    .body(Body::from(body.to_string())).unwrap();
  let res = client.request(req).await?;
  let status = res.status();

  let buf = hyper::body::to_bytes(res).await?;
  let body = String::from_utf8(buf.into_iter().collect()).unwrap();

  Ok((status, body))
}

impl StoreError for HttpStoreError {
  fn error_type(&self) -> StoreErrorType {
    StoreErrorType::UnknownError
  }
}

impl EventSource for HttpStore {
  type Connection = hyper::client::Client<HttpConnector, hyper::body::Body>;

  fn with_connection(connection: Self::Connection) -> Self {
    HttpStore { client: connection }
  }

  fn get_range(
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

impl<C: Command> CommandProcessor<C> for HttpStore {
  /// Apply a command to an aggregate.
  fn issue_command(&mut self, aggregate_id: &Uuid, command: &C)
    -> Result<Commit, Either<ClientError, C::Error>> {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (status, response_body) = rt.block_on(
      issue_post(
        &mut self.client,
        Uri::try_from(
          format!(
            "http://localhost:4321/commit/{}",
            aggregate_id
          )
        ).unwrap(),
        serde_json::to_value(command).unwrap()
      )
    ).unwrap();
    if status == http::status::StatusCode::UNPROCESSABLE_ENTITY {
      let body: HttpErrorResponse = serde_json::from_str(response_body.as_ref()).unwrap();
      let err: C::Error = serde_json::from_value(body.error).unwrap();

      return Err(either::Right(err));
    }

    let commit: Commit = serde_json::from_str(response_body.as_ref()).unwrap();
    Ok(commit)
  }
}


