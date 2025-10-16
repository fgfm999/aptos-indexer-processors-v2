use crate::processors::default::models::table_items::{
    CurrentTableItem, PostgresCurrentTableItem,
    TableItem,
};
use crate::processors::events::events_model::PostgresEvent;
use ahash::{AHashMap, AHashSet};
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::Transaction,
    traits::{AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
    aptos_protos::transaction::v1::write_set_change::Change as WriteSetChangeEnum
};
use async_trait::async_trait;

use crate::processors::events::parse_events;

pub struct CustomExtractor
where
    Self: Sized + Send + 'static,
{
    target_events: AHashSet<String>,
    target_prefix_events: AHashSet<String>,
}

impl CustomExtractor {
    pub fn new(target_events: AHashSet<String>, target_prefix_events: AHashSet<String>) -> Self {
        Self {
            target_events,
            target_prefix_events,
        }
    }
}

#[async_trait]
impl Processable for CustomExtractor {
    type Input = Vec<Transaction>;
    type Output = (Vec<PostgresEvent>, Vec<PostgresCurrentTableItem>);
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<
        Option<TransactionContext<(Vec<PostgresEvent>, Vec<PostgresCurrentTableItem>)>>,
        ProcessorError,
    > {
        let mut events = Vec::with_capacity(10);
        let mut current_table_items = AHashMap::new();

        let txns = transactions.data.clone();

        for txn in txns {
            let version = txn.version as i64;
            let block_height = txn.block_height as i64;
            let timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!");
            #[allow(deprecated)]
            let block_timestamp = chrono::NaiveDateTime::from_timestamp_opt(
                timestamp.seconds,
                timestamp.nanos as u32,
            )
            .expect("Txn Timestamp is invalid!");
            let transaction_info = txn.info.as_ref().expect("Transaction info doesn't exist!");

            for (index, wsc) in transaction_info.changes.iter().enumerate() {
                match wsc
                    .change
                    .as_ref()
                    .expect("WriteSetChange must have a change")
                {
                    WriteSetChangeEnum::WriteTableItem(inner) => {
                        let (_, cti) = TableItem::from_write_table_item(
                            inner,
                            index as i64,
                            version,
                            block_height,
                            block_timestamp,
                        );
                        current_table_items.insert(
                            (cti.table_handle.clone(), cti.key_hash.clone()),
                            cti.clone(),
                        );
                    },
                    WriteSetChangeEnum::DeleteTableItem(inner) => {
                        let (_, cti) = TableItem::from_delete_table_item(
                            inner,
                            index as i64,
                            version,
                            block_height,
                            block_timestamp,
                        );
                        current_table_items
                            .insert((cti.table_handle.clone(), cti.key_hash.clone()), cti);
                    },
                    _ => {},
                };
            }

            let txn_events = parse_events(&txn, self.name().as_str())
                .into_iter()
                .filter(|e| {
                    self.target_events.contains(&e.indexed_type)
                        || self
                            .target_prefix_events
                            .iter()
                            .any(|prefix| e.indexed_type.starts_with(prefix))
                })
                .map(Into::into);
            events.extend(txn_events);
        }

        let mut current_table_items = current_table_items
            .into_values()
            .collect::<Vec<CurrentTableItem>>();
        current_table_items
            .sort_by(|a, b| (&a.table_handle, &a.key_hash).cmp(&(&b.table_handle, &b.key_hash)));

        let current_table_items = current_table_items
            .into_iter()
            .map(PostgresCurrentTableItem::from)
            .collect();

        Ok(Some(TransactionContext {
            data: (events, current_table_items),
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for CustomExtractor {}

impl NamedStep for CustomExtractor {
    fn name(&self) -> String {
        "CustomExtractor".to_string()
    }
}
