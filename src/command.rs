use super::aggregate::Aggregate;
use std::error::Error;

pub trait Command {
  type Aggregate: Aggregate;
  type Error: Error;

  fn apply(
    &self,
    aggregate: &Self::Aggregate,
  ) -> Result<Vec<<<Self as Command>::Aggregate as Aggregate>::Event>, Self::Error>;
}
