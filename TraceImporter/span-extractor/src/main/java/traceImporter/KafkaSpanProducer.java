package traceImporter;

import com.google.common.primitives.Longs;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Writes the spans contained in a span dump into the kafka topic "cluster-spans"
 */
public class KafkaSpanProducer implements KafkaDumpSpanConsumer.DumpSpanHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpanProducer.class);
    private KafkaProducer<Long, EVSpan> kafkaProducer;

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
        properties.put("value.serializer", KafkaAvroSerializer.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        kafkaProducer = new KafkaProducer<>(properties);
    }

    @Override
    public void handle(DumpSpans dumpSpans) {
        for (Span span : dumpSpans.getSpansList()) {

            ByteBuffer spanIdBuf = ByteBuffer.wrap(span.getSpanId().toByteArray());
            ByteBuffer traceIdBuf = ByteBuffer.wrap(span.getTraceId().toByteArray());

            EVSpan evSpan = new EVSpan(spanIdBuf, traceIdBuf);

            Long id = Longs.fromByteArray(span.getTraceId().toByteArray());

            ProducerRecord<Long, EVSpan> spanRecord = new ProducerRecord<>(TOPIC, id, evSpan);

            Future<RecordMetadata> metadata = kafkaProducer.send(spanRecord);

            String hexID = Base64.getEncoder().encodeToString(span.getSpanId().toByteArray());

            LOGGER.info("Sent span with id {} ({})", id, hexID);

        }


    }


    private byte[] serialize(EVSpan span) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<EVSpan> spanDatumWriter = new SpecificDatumWriter<>(EVSpan.class);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        byte[] serialized = null;
        try {
            spanDatumWriter.write(span, encoder);
            encoder.flush();
            out.close();
            serialized = out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return serialized;

    }

}
