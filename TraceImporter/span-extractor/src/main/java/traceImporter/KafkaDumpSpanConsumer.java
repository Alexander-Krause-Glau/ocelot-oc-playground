package traceImporter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.AttributeValue;
import io.opencensus.proto.trace.v1.Span;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrives {@link DumpSpans} objects from the "cluster-dump-spans" topic
 */
public class KafkaDumpSpanConsumer implements Runnable {

  public interface DumpSpanHandler {
    void handle(DumpSpans dumpSpans);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDumpSpanConsumer.class);
  private static final String KAFKA_TOPIC = "cluster-dump-spans";

  private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private DumpSpanHandler handler;



  public KafkaDumpSpanConsumer() {
    this(ds -> System.out.println("First Span in dump: \n" + SpanToString(ds.getSpans(0))));

  }

  public KafkaDumpSpanConsumer(DumpSpanHandler handler) {

    final Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9091");
    properties.put("group.id", "trace-importer-1");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("key.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");// NOCS
    properties.put("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    this.kafkaConsumer = new KafkaConsumer<>(properties);
    this.handler = handler;

  }

  @Override
  public void run() {
    LOGGER.info("Starting Kafka Trace Consumer.\n");


    this.kafkaConsumer.subscribe(Arrays.asList(KAFKA_TOPIC));

    while (true) {
      final ConsumerRecords<byte[], byte[]> records =
          this.kafkaConsumer.poll(Duration.ofMillis(100));

      for (final ConsumerRecord<byte[], byte[]> record : records) {

        final byte[] serializedTrace = record.value();

        try {
          DumpSpans s = DumpSpans.parseFrom(serializedTrace);
          LOGGER.info("New dump span with {} spans ({} unique trace ids)\n", s.getSpansCount(),
              countUniqueTraceIds(s));
          handler.handle(s);

        } catch (InvalidProtocolBufferException e) {
          e.printStackTrace();
        }

      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }


  private int countUniqueTraceIds(DumpSpans ds) {
    int c = 0;
    List<ByteString> seen = new ArrayList<>(ds.getSpansCount());

    for (Span s : ds.getSpansList()) {
      if (!seen.contains(s.getTraceId())) {
        c++;
        seen.add(s.getTraceId());
      }
    }

    return c;
  }

  private static String SpanToString(Span s) {

    String spanId = Base64.getEncoder().encodeToString(s.getSpanId().toByteArray());
    String traceId = Base64.getEncoder().encodeToString(s.getTraceId().toByteArray());
    StringBuilder sb = new StringBuilder("{\n");


    sb.append("\tTraceID: ").append(traceId).append("\n");
    sb.append("\tSpanID: ").append(spanId).append("\n");
    sb.append("\tname: ").append(s.getName().getValue()).append("\n");
    sb.append("\tStart (seconds): ").append(s.getStartTime().getSeconds()).append("\n");
    sb.append("\tEnd (seconds): ").append(s.getEndTime().getSeconds()).append("\n");
    sb.append("\tAttributes{\n");
    for (Map.Entry<String, AttributeValue> entry : s.getAttributes().getAttributeMapMap()
        .entrySet()) {
      sb.append("\t\t").append(entry.getKey()).append(" -> ")
          .append(entry.getValue().getStringValue());
    }
    sb.append("\t}\n");


    sb.append("}\n");



    return sb.toString();
  }


}
