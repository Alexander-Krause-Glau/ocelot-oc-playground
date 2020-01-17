package traceImporter;

import com.google.common.primitives.Longs;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Writes the spans contained in a span dump into the kafka topic "cluster-spans"
 */
public class KafkaSpanProducer implements KafkaDumpSpanConsumer.DumpSpanHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpanProducer.class);
    private KafkaProducer<Long, byte[]> kafkaProducer;

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
        properties.put("key.serializer", LongSerializer.class);// NOCS
        properties.put("value.serializer", ByteArraySerializer.class);

        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void handle(DumpSpans dumpSpans) {
        for (Span span: dumpSpans.getSpansList()) {

            Long id = Longs.fromByteArray(span.getTraceId().toByteArray());
            byte[] serialized = span.toByteArray();


            ProducerRecord<Long, byte[]> spanRecord = new ProducerRecord<>(TOPIC, id, serialized);

            Future<RecordMetadata> metadata = kafkaProducer.send(spanRecord);

            //  String hexID = Base64.getEncoder().encodeToString(span.getSpanId().toByteArray());

                //LOGGER.info("Sent span with id {} ({}) to partition {}",
                //    id, hexID, metadata.get(1, TimeUnit.SECONDS));

        }


    }
}
