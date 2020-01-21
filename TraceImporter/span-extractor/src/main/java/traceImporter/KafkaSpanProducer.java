package traceImporter;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:8081");

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

      String traceid =
          BaseEncoding.base16().lowerCase().encode(span.getTraceId().toByteArray(), 0, 16);
      String spanid =
          BaseEncoding.base16().lowerCase().encode(span.getSpanId().toByteArray(), 0, 8);

      LOGGER.info("Sent span with trace id {} and span id {}", traceid, spanid);

      String hexID = Base64.getEncoder().encodeToString(span.getSpanId().toByteArray());
      final Charset UTF8_CHARSET = Charset.forName("UTF-8");
      // span.getSpanId().toString().toLowerBase16();
      // System.out.println(new String(span.getTraceId().toByteArray(), UTF8_CHARSET));

      // System.out.println(span.getSpanId().size());

      // String test = String.format("%02x", span.getSpanId().toByteArray());
      // System.out.println(test);

      // LOGGER.info("Sent span with trace id {} and span id {}, {})", id, hexID,
      // normalizeTraceId(span.getTraceId().toByteArray().toString()));

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

  //

  public static String normalizeTraceId(String traceId) {
    if (traceId == null)
      throw new NullPointerException("traceId == null");
    int length = traceId.length();
    if (length == 0)
      throw new IllegalArgumentException("traceId is empty");
    if (length > 32)
      throw new IllegalArgumentException("traceId.length > 32");
    int zeros = validateHexAndReturnZeroPrefix(traceId);
    if (zeros == length)
      throw new IllegalArgumentException("traceId is all zeros");
    if (length == 32 || length == 16) {
      if (length == 32 && zeros >= 16)
        return traceId.substring(16);
      return traceId;
    } else if (length < 16) {
      return padLeft(traceId, 16);
    } else {
      return padLeft(traceId, 32);
    }
  }

  static final String THIRTY_TWO_ZEROS;
  static {
    char[] zeros = new char[32];
    Arrays.fill(zeros, '0');
    THIRTY_TWO_ZEROS = new String(zeros);
  }

  static String padLeft(String id, int desiredLength) {
    int length = id.length();
    int numZeros = desiredLength - length;

    char[] data = shortStringBuffer();
    THIRTY_TWO_ZEROS.getChars(0, numZeros, data, 0);
    id.getChars(0, length, data, numZeros);

    return new String(data, 0, desiredLength);
  }

  static final ThreadLocal<char[]> SHORT_STRING_BUFFER = new ThreadLocal<>();
  /** Maximum character length constraint of most names, IP literals and IDs. */
  public static final int SHORT_STRING_LENGTH = 256;

  public static char[] shortStringBuffer() {
    char[] shortStringBuffer = SHORT_STRING_BUFFER.get();
    if (shortStringBuffer == null) {
      shortStringBuffer = new char[SHORT_STRING_LENGTH];
      SHORT_STRING_BUFFER.set(shortStringBuffer);
    }
    return shortStringBuffer;
  }


  static int validateHexAndReturnZeroPrefix(String id) {
    int zeros = 0;
    boolean inZeroPrefix = id.charAt(0) == '0';
    for (int i = 0, length = id.length(); i < length; i++) {
      char c = id.charAt(i);
      if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
        throw new IllegalArgumentException(id + " should be lower-hex encoded with no prefix");
      }
      if (c != '0') {
        inZeroPrefix = false;
      } else if (inZeroPrefix) {
        zeros++;
      }
    }
    return zeros;
  }

}
