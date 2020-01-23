package traceImporter;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

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
    StreamsBuilder builder = new StreamsBuilder();
    // KStream<byte[], byte[]> dumpSpanStream =
    // builder.stream(IN_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray())
    // .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
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

          result.add(KeyValue.pair(traceId, new EVSpan(spanId, traceId)));
        }

      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      return result;


    });

    TimeWindowedKStream<String, EVSpan> windowedStream =
        traceIdSpanStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofSeconds(10)));

    KTable<Windowed<String>, EVSpanList> messagesAggregatedByWindow = windowedStream
        .aggregate(() -> new EVSpanList(), new Aggregator<String, EVSpan, EVSpanList>() {

          @Override
          public EVSpanList apply(String key, EVSpan value, EVSpanList aggregate) {
            if (aggregate.getSpanList() == null) {
              aggregate.setSpanList(new ArrayList<EVSpan>());
            }
            aggregate.getSpanList().add(value);
            return aggregate;
          }
        });

    KStream<Windowed<String>, EVSpanList> spansWindowedStream =
        messagesAggregatedByWindow.toStream();

    spansWindowedStream.foreach(new ForeachAction<Windowed<String>, EVSpanList>() {
      public void apply(Windowed<String> key, EVSpanList value) {
        System.out.println(key.key() + ": " + value.getSpanList().get(0).getTraceId() + "; "
            + value.getSpanList().size());
      }
    });

    KStream<String, EVSpanList> transformed = spansWindowedStream
        .map(new KeyValueMapper<Windowed<String>, EVSpanList, KeyValue<String, EVSpanList>>() {
          @Override
          public KeyValue<String, EVSpanList> apply(Windowed<String> key, EVSpanList value) {
            return new KeyValue<>(key.key(), value);
          }
        });

    transformed.to(OUT_TOPIC);

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

