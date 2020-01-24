package traceImporter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanToTraceReconstructorStream {

  private static final String OUT_TOPIC = "span-batches";
  private static final String IN_TOPIC = "explorviz-spans";

  private final Properties streamsConfig = new Properties();

  public SpanToTraceReconstructorStream() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

    streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        EVSpanTimestampKafkaExtractor.class);
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "explorviz-span-batching");
  }


  public void run() {

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, EVSpan> explSpanStream = builder.stream(IN_TOPIC);

    TimeWindowedKStream<String, EVSpan> windowedStream =
        explSpanStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofSeconds(10)));

    KTable<Windowed<String>, EVSpanList> messagesAggregatedByWindow = windowedStream
        .aggregate(EVSpanList::new, (key, value, aggregate) -> {
          if (aggregate.getSpanList() == null) {
            aggregate.setSpanList(new ArrayList<>());
          }
          aggregate.getSpanList().add(value);
          return aggregate;
        });

    KStream<Windowed<String>, EVSpanList> spansWindowedStream =
        messagesAggregatedByWindow.toStream();

    spansWindowedStream.foreach(
        (key, value) -> System.out.printf("New trace with %d spans (id: %s)\n",
            value.getSpanList().size(), key));

    KStream<String, EVSpanList> transformed = spansWindowedStream
        .map(
            (KeyValueMapper<Windowed<String>, EVSpanList, KeyValue<String, EVSpanList>>) (key, value) -> new KeyValue<>(key.key(), value));

    transformed.to(OUT_TOPIC);


    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}

