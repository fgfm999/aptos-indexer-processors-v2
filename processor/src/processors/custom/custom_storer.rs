use crate::processors::custom::custom_processor::CustomProcessorConfig;
use crate::{
    processors::default::default_storer::insert_current_table_items_query,
    processors::default::models::table_items::PostgresCurrentTableItem,
};
use ahash::AHashMap;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use crate::processors::custom::models::event::PostgresEvent;

use crate::schema;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};


pub struct CustomStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: CustomProcessorConfig,
}

impl CustomStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: CustomProcessorConfig,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
        }
    }
}

#[async_trait]
impl Processable for CustomStorer {
    type Input = (Vec<PostgresEvent>,Vec<PostgresCurrentTableItem>,);
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<(Vec<PostgresEvent>,Vec<PostgresCurrentTableItem>)>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let (events, current_table_items) = input.data;

        let per_table_chunk_sizes: AHashMap<String, usize> =
            self.processor_config.per_table_chunk_sizes.clone();


        let events_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_events_query,
            &events,
            get_config_table_chunk_size::<PostgresEvent>("events", &per_table_chunk_sizes),
        );

        let current_table_items_res = execute_in_chunks(
            self.conn_pool.clone(),
            insert_current_table_items_query,
            &current_table_items,
            get_config_table_chunk_size::<PostgresCurrentTableItem>(
                "current_table_items",
                &per_table_chunk_sizes,
            ),
        );

        futures::try_join!(
            events_res,
            current_table_items_res
        )?;

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for CustomStorer {}

impl NamedStep for CustomStorer {
    fn name(&self) -> String {
        "CustomStorer".to_string()
    }
}


pub fn insert_events_query(
    items_to_insert: Vec<PostgresEvent>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::events::dsl::*;

    diesel::insert_into(schema::events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_update()
        .set((
            inserted_at.eq(excluded(inserted_at)),
            indexed_type.eq(excluded(indexed_type)),
        ))
}
