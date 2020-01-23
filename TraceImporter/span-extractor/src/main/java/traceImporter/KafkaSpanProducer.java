package traceImporter;

import com.google.common.io.BaseEncoding;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the spans contained in a span dump into the kafka topic "cluster-spans"
 */
public class KafkaSpanProducer implements KafkaDumpSpanConsumer.DumpSpanHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpanProducer.class);
  private KafkaProducer<String, EVSpan> kafkaProducer;

  private static final String TOPIC = "cluster-spans";


  public KafkaSpanProducer() {
    final Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9091; 0.0.0.0:9091");
    properties.put("acks", "all");
    properties.put("retries", "1");
    properties.put("batch.size", "16384");
    properties.put("linger.ms", "1");
    properties.put("max.request.size", "2097152");
    properties.put("buffer.memory", 33_554_432); // NOCS
    properties.put("key.serializer", StringSerializer.class);// NOCS
    properties.put("value.serializer", KafkaAvroSerializer.class);
    properties.put("schema.registry.url", "http://localhost:8081");

    kafkaProducer = new KafkaProducer<>(properties);
  }

  @Override
  public void handle(DumpSpans dumpSpans) {
    for (Span span : dumpSpans.getSpansList()) {

      String traceid =
          BaseEncoding.base16().lowerCase().encode(span.getTraceId().toByteArray(), 0, 16);
      String spanid =
          BaseEncoding.base16().lowerCase().encode(span.getSpanId().toByteArray(), 0, 8);

      EVSpan evSpan = new EVSpan(spanid, traceid);

      ProducerRecord<String, EVSpan> spanRecord = new ProducerRecord<>(TOPIC, traceid, evSpan);

      kafkaProducer.send(spanRecord);

      LOGGER.info("Sent span with trace id {} and span id {}", traceid, spanid);
    }
  }

}
