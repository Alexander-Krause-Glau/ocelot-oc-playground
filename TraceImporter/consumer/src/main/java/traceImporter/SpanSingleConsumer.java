package traceImporter;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpanSingleConsumer implements Runnable {

  private static final String TOPIC = "span-batches";
  private static final Logger LOGGER = LoggerFactory.getLogger(SpanSingleConsumer.class);
  KafkaConsumer<Long, EVSpan> consumer;

  public SpanSingleConsumer() {
    final Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9091");
    properties.put("group.id", "trace-importer-1");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");

    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:8081");
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    this.consumer = new KafkaConsumer<>(properties);
  }


  @Override
  public void run() {
    this.consumer.subscribe(Arrays.asList(TOPIC));


    while (true) {
      ConsumerRecords<Long, EVSpan> records = this.consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<Long, EVSpan> rec : records) {
        EVSpan s = rec.value();
        LOGGER.info("New span with trace id {} and span id {}", toBase64(s.getTraceId()),
            toBase64(s.getSpanId()));
      }
    }
  }

  private String toBase64(ByteBuffer byteString) {
    return Base64.getEncoder().encodeToString(byteString.array());
  }
}
