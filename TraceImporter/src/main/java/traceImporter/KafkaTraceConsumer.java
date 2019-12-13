package traceImporter;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import io.opencensus.proto.trace.v1.TraceProto;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanId;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

import io.opencensus.trace.export.SpanData;
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
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");// NOCS
    properties.put("value.deserializer",
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");

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

        System.out.println("KEY: " + Arrays.toString(record.key()));
        // LOGGER.info("Recevied Kafka record: {}", record.value());

        final byte[] serializedTrace = record.value();

        //System.out.printf("SpanId: %s, TraceId: %s\n", s.getSpanId().toStringUtf8(), s.getTraceId().toStringUtf8());

        try {
          DumpSpans s = DumpSpans.parseFrom(serializedTrace);
          System.out.printf("Spans: %d\n", s.getSpansCount());

          System.out.printf("Span 0 of bundle:\n%s\n\n", SpanToString(s.getSpans(0)));

        } catch (InvalidProtocolBufferException | UnsupportedEncodingException e) {
          e.printStackTrace();
        }

      }
    }

  }


  private String SpanToString(Span s) throws UnsupportedEncodingException {

    String spanId = Base64.getEncoder().encodeToString(s.getSpanId().toByteArray());
    String traceId = Base64.getEncoder().encodeToString(s.getTraceId().toByteArray());
    StringBuilder sb = new StringBuilder("{\n");


    sb.append("\tTraceID: ").append(traceId).append("\n");
    sb.append("\tSpanID: ").append(spanId).append("\n");
    sb.append("\tname: ").append(s.getName().getValue()).append("\n");
    sb.append("\tStart (seconds): ").append(s.getStartTime().getSeconds()).append("\n");
    sb.append("\tEnd (seconds): ").append(s.getEndTime().getSeconds()).append("\n");

    sb.append("}\n");



    return sb.toString();
  }


}
