package traceImporter;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.trace.v1.Span;
import io.opencensus.trace.SpanId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.opencensus.proto.agent.trace.v1.TraceServiceGrpc.TraceServiceStub;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaTraceConsumer implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTraceConsumer.class);
  private static final String KAFKA_TOPIC = "";

  private final KafkaConsumer<byte[], byte[]> kafkaConsumer;


  public KafkaTraceConsumer() {

    final Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9091");
    properties.put("group.id", "trace-importer-1");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer", ByteArrayDeserializer.class.getName());// NOCS
    properties.put("value.deserializer", ByteArrayDeserializer.class.getName());

    this.kafkaConsumer = new KafkaConsumer<>(properties);
  }

  @Override
  public void run() {
    LOGGER.info("Starting Kafka Trace Consumer.\n");


    this.kafkaConsumer.subscribe(Arrays.asList(KAFKA_TOPIC));

    while (true) {
      final ConsumerRecords<byte[], byte[]> records =
          this.kafkaConsumer.poll(Duration.ofMillis(100));

      for (final ConsumerRecord<byte[], byte[]> record : records) {

        // LOGGER.info("Recevied Kafka record: {}", record.value());

        final byte[] serializedTrace = record.value();


        final ByteBuffer buffer = ByteBuffer.wrap(serializedTrace);

        System.out.println(Arrays.toString(serializedTrace));


        int spanIdSize = SpanId.SIZE;

        final byte[] bytes = new byte[8];
        buffer.get(bytes);

        ByteBuffer bufferLong = ByteBuffer.allocate(Long.BYTES);
        bufferLong.put(bytes);
        bufferLong.flip();// need flip
        long x = bufferLong.getLong();
        Gson g = new Gson();

        // String spanId = new String(bytes, Charset.forName("UTF-8"));

        // SpanId test = SpanId.fromBytes(bytes);

        // System.out.println(test);

        try {
          Span span = Span.parseFrom(serializedTrace);
          System.out.printf("\n New Span:\n%s\n\n", g.toJson(span));
          System.out.printf("TraceId: %s\n\n", span.getTraceId().toStringUtf8());
        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }

      }
    }

  }


}
