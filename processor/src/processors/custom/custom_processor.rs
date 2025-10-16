use ahash::{AHashMap, AHashSet};
use aptos_indexer_processor_sdk::{
    builder::ProcessorBuilder,
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    common_steps::{TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS},
    postgres::utils::checkpoint::PostgresChainIdChecker,
    postgres::utils::database::{new_db_pool, ArcDbPool},
    traits::IntoRunnableStep,
    traits::processor_trait::ProcessorTrait,
    utils::chain_id_check::check_or_update_chain_id
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use crate::{
    processors::custom::custom_extractor::CustomExtractor,
    config::processor_config::ProcessorConfig,
    config::indexer_processor_config::IndexerProcessorConfig,
    config::db_config::DbConfig,
    processors::custom::custom_storer::CustomStorer,
    processors::processor_status_saver::{get_end_version, get_starting_version, PostgresProcessorStatusSaver}
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CustomProcessorConfig {
    // Number of rows to insert, per chunk, for each DB table. Default per table is ~32,768 (2**16/2)
    #[serde(default = "AHashMap::new")]
    pub per_table_chunk_sizes: AHashMap<String, usize>,
    // Size of channel between steps
    #[serde(default = "CustomProcessorConfig::default_channel_size")]
    pub channel_size: usize,
    // String vector for tables to write to DB, by default all tables are written
    #[serde(default)]
    target_events: AHashSet<String>,
    #[serde(default)]
    target_prefix_events: AHashSet<String>,
}
impl CustomProcessorConfig {
    pub const fn default_channel_size() -> usize {
        10
    }
}

pub struct CustomProcessor {
    pub config: IndexerProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl CustomProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> anyhow::Result<Self> {
        match config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                let conn_pool = new_db_pool(
                    &postgres_config.connection_string,
                    Some(postgres_config.db_pool_size),
                )
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                        "Failed to create connection pool for PostgresConfig: {:?}",
                        e
                    )
                    })?;

                Ok(Self {
                    config,
                    db_pool: conn_pool,
                })
            },
            _ => Err(anyhow::anyhow!(
                "Invalid db config for CustomProcessor {:?}",
                config.db_config
            )),
        }
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for CustomProcessor {
    fn name(&self) -> &'static str {
        self.config.processor_config.name()
    }

    async fn run_processor(&self) -> anyhow::Result<()> {
        // Implement your custom processing logic here
        // Run migrations
        // if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
        //     run_migrations(
        //         postgres_config.connection_string.clone(),
        //         self.db_pool.clone(),
        //         MIGRATIONS,
        //     )
        //         .await;
        // }

        //  Merge the starting version from config and the latest processed version from the DB
        let (starting_version, ending_version) = (
            get_starting_version(&self.config, self.db_pool.clone()).await?,
            get_end_version(&self.config, self.db_pool.clone()).await?,
        );

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
            .await?;

        let processor_config = match self.config.processor_config.clone() {
            ProcessorConfig::CustomProcessor(processor_config) => processor_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid processor config for UserTransactionProcessor: {:?}",
                    self.config.processor_config
                ))
            },
        };
        info!(
            "Starting CustomProcessor with config {:?}",
            processor_config
        );
        let channel_size = processor_config.channel_size;

        // Define processor steps
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
            .await?;

        let custom_extractor = CustomExtractor::new(processor_config.target_events.clone(), processor_config.target_prefix_events.clone());
        let custom_store = CustomStorer::new(self.db_pool.clone(), processor_config);
        let version_tracker = VersionTrackerStep::new(
            PostgresProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
            .connect_to(custom_extractor.into_runnable_step(), channel_size)
            .connect_to(custom_store.into_runnable_step(), channel_size)
            .connect_to(version_tracker.into_runnable_step(), channel_size)
            .end_and_return_output_receiver(channel_size);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    debug!(
                        "Finished processing versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                },
                Err(e) => {
                    info!("No more transactions in channel: {:?}", e);
                    break Ok(());
                },
            }
        }


    }
}
