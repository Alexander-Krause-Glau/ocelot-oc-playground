package traceImporter;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Translates opencensus {@link Span} objects to {@link EVSpan}s.
 */
public class SpanTranslator {

  private static final String IN_TOPIC = "cluster-dump-spans";
  private static final String OUT_TOPIC = "explorviz-spans";

  private final Properties streamsConfig = new Properties();

  public SpanTranslator() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "span-batching");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
  }


  public void run() {
    StreamsBuilder builder = new StreamsBuilder();

    // Stream 1

    KStream<byte[], byte[]> dumpSpanStream =
        builder.stream(IN_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));
    KStream<String, EVSpan> traceIdSpanStream = dumpSpanStream.flatMap((key, value) -> {

      DumpSpans dumpSpan;
      List<KeyValue<String, EVSpan>> result = new LinkedList<>();
      try {

        dumpSpan = DumpSpans.parseFrom(value);

        for (Span s : dumpSpan.getSpansList()) {
          String traceId =
              BaseEncoding.base16().lowerCase().encode(s.getTraceId().toByteArray(), 0, 16);

          String spanId =
              BaseEncoding.base16().lowerCase().encode(s.getSpanId().toByteArray(), 0, 8);

          long timestamp = s.getStartTime().getNanos() / 1000000;

          result.add(KeyValue.pair(traceId, new EVSpan(spanId, traceId, timestamp)));
          // result.add(KeyValue.pair(traceId, new EVSpan(spanId, traceId, timestamp, 2)));
        }

      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return result;


    });

    traceIdSpanStream.to(OUT_TOPIC);
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}

