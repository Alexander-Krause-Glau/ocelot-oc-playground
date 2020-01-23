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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanToTraceReconstructorStream {

  private static final String IN_TOPIC = "cluster-dump-spans";
  private static final String OUT_TOPIC = "span-batches";

  private final Properties streamsConfig = new Properties();

  public SpanToTraceReconstructorStream() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "span-batching");
    // streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
    // "org.apache.kafka.streams.processor.WallclockTimestampExtractor");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

  }


  public void run() {
    System.out.println("test1");
    StreamsBuilder builder = new StreamsBuilder();
    KStream<byte[], byte[]> dumpSpanStream =
        builder.stream(IN_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray())
            .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
    System.out.println("test2");
    KStream<String, EVSpan> traceIdSpanStream = dumpSpanStream.flatMap((key, value) -> {

      DumpSpans dumpSpan;
      List<KeyValue<String, EVSpan>> result = new LinkedList<>();
      try {
        System.out.println("test");

        dumpSpan = DumpSpans.parseFrom(value);

        for (Span s : dumpSpan.getSpansList()) {
          String traceId =
              BaseEncoding.base16().lowerCase().encode(s.getTraceId().toByteArray(), 0, 16);

          String spanId =
              BaseEncoding.base16().lowerCase().encode(s.getSpanId().toByteArray(), 0, 8);

          result.add(KeyValue.pair(traceId, new EVSpan(spanId, traceId)));
        }

      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return result;


    });

    traceIdSpanStream.foreach(new ForeachAction<String, EVSpan>() {
      public void apply(String key, EVSpan value) {
        System.out.println(key + ": " + value.getSpanId());
      }
    });

    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
      }
    });
  }

}

