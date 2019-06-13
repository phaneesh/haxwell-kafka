package com.hbase.haxwell;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hbase.haxwell.api.HaxwellEvent;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.config.HaxwellConfig;
import com.hbase.haxwell.core.HaxwellColumn;
import com.hbase.haxwell.core.HaxwellRow;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

@Log4j
public class KafkaEventListener implements HaxwellEventListener {

  @Getter
  private final KafkaProducer<String, String> producer;

  private final HaxwellConfig haxwellConfig;

  private final ObjectMapper objectMapper = new ObjectMapper();

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
  public void processEvents(List<HaxwellEvent> haxwellEvents) {
    for(HaxwellEvent event : haxwellEvents) {
      final String tableName = Bytes.toString(event.getTable());
      if(haxwellConfig.getTableName().equals("*") || tableName.equals(haxwellConfig.getTableName())) {
        event.getKeyValues().forEach( cell -> {
          String type = null;
          HaxwellColumn column = null;
          if(cell.getTypeByte() == Type.Put.getCode()) {
            type = "PUT";
            column = HaxwellColumn.builder()
                .columnFamily(Bytes.toString(CellUtil.cloneFamily(cell)))
                .columnName(Bytes.toString(CellUtil.cloneQualifier(cell)))
                .value(Bytes.toString(CellUtil.cloneValue(cell)))
                .build();
          }
          if(cell.getTypeByte() == Type.Delete.getCode()) {
            type = "DELETE";
          }
          if(type != null) {
            final String id = Bytes.toString(CellUtil.cloneRow(cell));
            HaxwellRow row = HaxwellRow.builder()
                .tableName(tableName)
                .id(id)
                .operation(type)
                .column(column)
                .build();
            try {
              switch (haxwellConfig.getTopicMode()) {
                case SINGLE_TOPIC:
                  producer.send(new ProducerRecord<>(haxwellConfig.getTopicName(), null, id,
                      objectMapper.writeValueAsString(row))).get();
                  break;
                case TOPIC_PER_TABLE:
                  producer.send(new ProducerRecord<>(tableName, null, id,
                      objectMapper.writeValueAsString(row))).get();
              }
              log.info("Published message with id: " +id);
            } catch(Exception e) {
              log.error("Error processing event:", e);
            }
          }
        });
      }

    }
  }
}
