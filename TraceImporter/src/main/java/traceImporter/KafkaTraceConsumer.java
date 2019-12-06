package traceImporter;

import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanId;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTraceConsumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTraceConsumer.class);
  private static final String KAFKA_TOPIC = "cluster-spans";

  private final KafkaConsumer<String, byte[]> kafkaConsumer;


  public KafkaTraceConsumer() {

    final Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9091");
    properties.put("group.id", "trace-importer-1");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// NOCS
    properties.put("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    this.kafkaConsumer = new KafkaConsumer<>(properties);
  }

  @Override
  public void run() {
    LOGGER.info("Starting Kafka Trace Consumer.\n");

    this.kafkaConsumer.subscribe(Arrays.asList(KAFKA_TOPIC));

    while (true) {
      final ConsumerRecords<String, byte[]> records =
          this.kafkaConsumer.poll(Duration.ofMillis(100));

      for (final ConsumerRecord<String, byte[]> record : records) {

        // LOGGER.info("Recevied Kafka record: {}", record.value());

        final byte[] serializedTrace = record.value();

        final ByteBuffer buffer = ByteBuffer.wrap(serializedTrace);

        int spanIdSize = SpanId.SIZE;

        final byte[] bytes = new byte[8];
        buffer.get(bytes);

        ByteBuffer bufferLong = ByteBuffer.allocate(Long.BYTES);
        bufferLong.put(bytes);
        bufferLong.flip();// need flip
        long x = bufferLong.getLong();

        // String spanId = new String(bytes, Charset.forName("UTF-8"));

        // SpanId test = SpanId.fromBytes(bytes);

        // System.out.println(test);

        System.out.println(x);

      }
    }

  }

}
