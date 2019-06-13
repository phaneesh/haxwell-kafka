package com.hbase.haxwell.config;

import lombok.Builder.Default;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author phaneesh
 */
@Data
@ToString
@EqualsAndHashCode
public class HaxwellConfig {

    public enum TopicMode {
        TOPIC_PER_TABLE,
        SINGLE_TOPIC
    }

    private String brokerList;

    private String clientId;

    private int retries = 3;

    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    private TopicMode topicMode = TopicMode.TOPIC_PER_TABLE;

    private String topicName;

    private String tableName;
}
