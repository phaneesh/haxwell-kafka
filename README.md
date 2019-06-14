Haxwell Kafka Relay
-------------------
Simple WALEntry listener and Kafka producer just like maxwell (for MySQL)

Configuration
-------------
Configuration is set via environment variables
 * ```HAXWELL_ZOOKEEPER_CONNECTION_STRING``` (Zookeeper connection string)
 * ```HAXWELL_ZOOKEEPER_CONNECTION_TIMEOUT``` (Session timeout for zookeeper)
 * ```HAXWELL_SUBSCRIPTION_NAME``` (Identify the subscriber with a name)
 * ```HAXWELL_HBASE_ZNODE``` (Parent znode which haxwell uses to join as a replica region)
 * ```HAXWELL_KAFKA_BROKERS``` (Kafka broker list)
 * ```HAXWELL_TOPIC_MODE``` (TOPIC_PER_TABLE/SINGLE_TOPIC)
 * ```HAXWELL_TOPIC_NAME``` (Used when SINGLE_TOPIC mode is used)
 * ```HAXWELL_TABLE_NAME``` (Defaults to all tables)
