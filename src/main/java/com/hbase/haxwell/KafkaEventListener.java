package com.hbase.haxwell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.core.HaxwellRow;
import com.hbase.haxwell.config.HaxwellConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Properties;

@Log4j
public class KafkaEventListener implements HaxwellEventListener {

  @Getter
  private final KafkaProducer<String, String> producer;

  private final HaxwellConfig haxwellConfig;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Builder
  public KafkaEventListener(HaxwellConfig haxwellConfig) {
    this.haxwellConfig = haxwellConfig;
    Properties props = new Properties();
    props.put("bootstrap.servers", haxwellConfig.getBrokerList());
    props.put("acks", "all");
    props.put("retries", haxwellConfig.getRetries());
    props.put("linger.ms", 0);
    props.put("key.serializer", haxwellConfig.getKeySerializer());
    props.put("value.serializer", haxwellConfig.getValueSerializer());
    producer = new KafkaProducer<>(props);
  }

  @Override
  public void processEvents(List<HaxwellRow> haxwellRows) {
    for (HaxwellRow event : haxwellRows) {
      final String tableName = event.getTableName();
      if (haxwellConfig.getTableName().equals("*") || tableName.equals(haxwellConfig.getTableName())) {
        try {
          RecordMetadata recordMetadata = null;
          switch (haxwellConfig.getTopicMode()) {
            case SINGLE_TOPIC:
              recordMetadata = producer.send(new ProducerRecord<>(haxwellConfig.getTopicName(), null, event.getId(),
                  objectMapper.writeValueAsString(event))).get();
              break;
            case TOPIC_PER_TABLE:
              recordMetadata = producer.send(new ProducerRecord<>(tableName, null, event.getId(),
                  objectMapper.writeValueAsString(event))).get();
          }
          if (log.isDebugEnabled()) {
            log.debug("Published message with row key: " + event.getId()
                + " | Offset: " + recordMetadata.offset() + " | Partition:" + recordMetadata.partition());
          }
        } catch (Exception e) {
          log.error("Error processing event:", e);
        }
      }
    }
  }
}
