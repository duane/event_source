extern crate tokio;

use chrono::{DateTime, Utc};
use commit::{Commit, CommitAttempt};
use dynomite::{
  dynamodb::{
    AttributeDefinition, AttributeValue, CreateTableError, CreateTableInput, DynamoDb,
    DynamoDbClient, GetItemInput, KeySchemaElement,
    ProvisionedThroughput, PutItemError, PutItemInput, PutItemOutput, QueryInput,
  },
  retry::{Policy, RetryingDynamoDb},
  FromAttributes, Item,
};
use futures::Future;
use rusoto_core::Region;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
  pub table_name: String,
}

impl Default for DynamoDbConfig {
  fn default() -> Self {
    DynamoDbConfig {
      table_name: String::from("commits"),
    }
  }
}

pub struct DynamoDbStore {
  pub client: RetryingDynamoDb<DynamoDbClient>,
  pub config: DynamoDbConfig,
}

impl Default for DynamoDbStore {
  fn default() -> Self {
    DynamoDbStore {
      config: DynamoDbConfig::default(),
      client: RetryingDynamoDb::new(DynamoDbClient::new(Region::default()), Policy::default()),
    }
  }
}

#[derive(Debug, Clone, Item)]
struct CommitDTO {
  #[hash]
  pub aggregate_id: Uuid,
  pub aggregate_version: i64,
  pub commit_id: Uuid,
  pub commit_timestamp: String,

  #[range]
  pub commit_sequence: i64,

  pub serialized_events: Vec<u8>,
  pub serialized_metadata: Vec<u8>,
  pub events_count: i64,
}

impl DynamoDbStore {
  pub fn initialize(
    &self,
  ) -> impl Future<Item = (), Error = rusoto_core::RusotoError<CreateTableError>> {
    let attribute_definitions = vec![
      AttributeDefinition {
        attribute_name: "aggregate_id".into(),
        attribute_type: "S".into(),
      },
      AttributeDefinition {
        attribute_name: "commit_sequence".into(),
        attribute_type: "N".into(),
      },
    ];
    let key_schema = vec![
      KeySchemaElement {
        attribute_name: String::from("aggregate_id"),
        key_type: "HASH".into(),
      },
      KeySchemaElement {
        attribute_name: String::from("commit_sequence"),
        key_type: "RANGE".into(),
      },
    ];
    self
      .client
      .create_table(CreateTableInput {
        attribute_definitions,
        provisioned_throughput: Some(ProvisionedThroughput {
          read_capacity_units: 1,
          write_capacity_units: 1,
        }),
        key_schema,
        table_name: self.config.table_name.clone(),
        ..CreateTableInput::default()
      })
      .map(|_| ())
  }

  pub fn commit(
    &mut self,
    commit_attempt: &CommitAttempt,
  ) -> impl Future<Item = PutItemOutput, Error = rusoto_core::RusotoError<PutItemError>> {
    let commit_dto = CommitDTO {
      aggregate_id: commit_attempt.aggregate_id,
      aggregate_version: commit_attempt.aggregate_version,
      commit_id: commit_attempt.commit_id,
      commit_sequence: commit_attempt.commit_sequence,
      commit_timestamp: commit_attempt.commit_timestamp.to_rfc3339(),
      serialized_events: commit_attempt.serialized_events.clone(),
      serialized_metadata: commit_attempt.serialized_metadata.clone(),
      events_count: commit_attempt.events_count,
    };
    self.client.put_item(PutItemInput {
      condition_expression: Some("attribute_not_exists(commit_sequence)".into()),
      conditional_operator: None,
      expected: None,
      expression_attribute_names: None,
      expression_attribute_values: None,
      item: commit_dto.into(),
      return_consumed_capacity: None,
      return_item_collection_metrics: None,
      return_values: None,
      table_name: self.config.table_name.clone(),
    })
  }

  pub fn get_commit(
    &mut self,
    aggregate_id: Uuid,
    commit_sequence: i64,
  ) -> impl Future<Item = Option<Commit>, Error = rusoto_core::RusotoError<rusoto_dynamodb::GetItemError>>
  {
    let mut key: HashMap<String, AttributeValue> = Default::default();
    let mut hash_value: AttributeValue = Default::default();
    hash_value.s = Some(aggregate_id.to_string());
    let mut range_value: AttributeValue = Default::default();
    range_value.n = Some(commit_sequence.to_string());
    key.insert("aggregate_id".into(), hash_value);
    key.insert("commit_sequence".into(), range_value);
    self
      .client
      .get_item(GetItemInput {
        consistent_read: Some(true),
        key,
        table_name: self.config.table_name.clone(),
        ..GetItemInput::default()
      })
      .map(|result| {
        result.item.map(|item| {
          let commit_dto = CommitDTO::from_attrs(item).expect("could not parse dynamo db row");

          Commit {
            aggregate_id: commit_dto.aggregate_id,
            aggregate_version: commit_dto.aggregate_version,
            commit_id: commit_dto.commit_id,
            commit_timestamp: DateTime::parse_from_rfc3339(&commit_dto.commit_timestamp)
              .expect("could not parse timestamp")
              .with_timezone(&Utc),
            commit_sequence: commit_dto.commit_sequence,
            commit_number: commit_dto.commit_sequence, // this is intentional
            serialized_events: commit_dto.serialized_events,
            serialized_metadata: commit_dto.serialized_metadata,
            events_count: commit_dto.events_count,
            dispatched: true,
          }
        })
      })
  }

  pub fn get_range(
    &self,
    aggregate_id: Uuid,
    min_commit_sequence: i64,
    max_commit_sequence: i64,
  ) -> impl Future<
    Item = Option<Vec<Commit>>,
    Error = rusoto_core::RusotoError<rusoto_dynamodb::QueryError>,
  > {
    let mut expression_attribute_values: HashMap<String, AttributeValue> = Default::default();

    let mut hash_value: AttributeValue = Default::default();
    hash_value.s = Some(aggregate_id.to_string());

    let mut min_range_value: AttributeValue = Default::default();
    min_range_value.n = Some(min_commit_sequence.to_string());

    let mut max_range_value: AttributeValue = Default::default();
    max_range_value.n = Some(max_commit_sequence.to_string());

    expression_attribute_values.insert(":aggregate_id".into(), hash_value);
    expression_attribute_values.insert(":commit_sequence_lower_bound".into(), min_range_value);
    expression_attribute_values.insert(":commit_sequence_upper_bound".into(), max_range_value);
    self
      .client
      .query(QueryInput {
        consistent_read: Some(true),
        key_condition_expression: Some("aggregate_id = :aggregate_id AND commit_sequence BETWEEN :commit_sequence_lower_bound AND :commit_sequence_upper_bound".into()),
        expression_attribute_values: Some(expression_attribute_values),
        table_name: self.config.table_name.clone(),
        ..QueryInput::default()
      })
      .map(|result| {
        result.items.map(|item_vec| {
          item_vec.into_iter().map(|item| {
            let commit_dto = CommitDTO::from_attrs(item).expect("could not parse dynamo db row");

            Commit {
              aggregate_id: commit_dto.aggregate_id,
              aggregate_version: commit_dto.aggregate_version,
              commit_id: commit_dto.commit_id,
              commit_timestamp: DateTime::parse_from_rfc3339(&commit_dto.commit_timestamp)
                .expect("could not parse timestamp")
                .with_timezone(&Utc),
              commit_sequence: commit_dto.commit_sequence,
              commit_number: commit_dto.commit_sequence, // this is intentional
              serialized_events: commit_dto.serialized_events,
              serialized_metadata: commit_dto.serialized_metadata,
              events_count: commit_dto.events_count,
              dispatched: true,
            }
          }).collect()
        })
      })
  }
}
