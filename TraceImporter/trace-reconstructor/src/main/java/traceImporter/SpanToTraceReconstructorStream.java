package traceImporter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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

  private static final String OUT_TOPIC = "explorviz-traces";
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

    KTable<Windowed<String>, Trace> messagesAggregatedByWindow =
        windowedStream.aggregate(Trace::new, (traceId, evSpan, trace) -> {

          long evSpanStartTime = evSpan.getStartTime();
          long evSpanEndTime = evSpan.getEndTime();

          if (trace.getSpanList() == null) {
            trace.setSpanList(new ArrayList<>());
            trace.getSpanList().add(evSpan);

            trace.setStartTime(evSpanStartTime);
            trace.setEndTime(evSpanEndTime);
            trace.setOverallRequestCount(1);
            trace.setDuration(evSpanEndTime - evSpanStartTime);

            trace.setTraceId(evSpan.getTraceId());
          } else {

            // TODO
            // Implement
            // - traceDuration
            // - Tracesteps with caller callee each = EVSpan


            // Find duplicates in Trace (via fqn), aggregate based on request count
            // Furthermore, potentially update trace values

            long newStartTime = trace.getStartTime();
            long newEndTime = trace.getEndTime();

            for (int i = 0; i < trace.getSpanList().size(); i++) {
              EVSpan includedSpan = trace.getSpanList().get(i);

              if (includedSpan.getOperationName().equals(evSpan.getOperationName())) {
                includedSpan.setRequestCount(includedSpan.getRequestCount() + 1);

                if (includedSpan.getStartTime() > evSpanStartTime) {
                  // System.out.println(
                  // "Updated from start " + trace.getStartTime() + " to " + evSpanStartTime);
                  includedSpan.setStartTime(evSpanStartTime);

                }

                if (includedSpan.getEndTime() < evSpanEndTime) {
                  // System.out
                  // .println("Updated from end " + trace.getEndTime() + " to " + evSpanEndTime);
                  includedSpan.setEndTime(evSpanEndTime);
                }

                break;
              } else if (i + 1 == trace.getSpanList().size()) {
                // currently comparing to last entry, which is not equal, therefore
                // add the span to the list
                trace.getSpanList().add(evSpan);
              }

              // update trace values
              if (trace.getStartTime() > evSpanStartTime) {
                // System.out.println(
                // "Updated from start " + trace.getStartTime() + " to " + evSpanStartTime);
                trace.setStartTime(evSpanStartTime);

              }

              if (trace.getEndTime() < evSpanEndTime) {
                // System.out
                // .println("Updated from end " + trace.getEndTime() + " to " + evSpanEndTime);
                trace.setEndTime(evSpanEndTime);
              }

              trace.setOverallRequestCount(trace.getOverallRequestCount() + 1);

              trace.setDuration(trace.getEndTime() - trace.getStartTime());
            }
          }

          return trace;
        });

    KStream<Windowed<String>, Trace> spansWindowedStream = messagesAggregatedByWindow.toStream();

    // spansWindowedStream.foreach((key, value) -> System.out
    // .printf("New trace with %d spans (id: %s)\n", value.getSpanList().size(), key));

    spansWindowedStream.foreach((key, value) -> {

      System.out.printf("New trace with %d spans (id: %s)\n", value.getSpanList().size(), key);

      List<EVSpan> list = value.getSpanList();

      list.forEach((val) -> {
        System.out.println(val.getStartTime() + " : " + val.getEndTime() + " für "
            + val.getOperationName() + " mit Anzahl " + val.getRequestCount());
      });

    });

    KStream<String, Trace> traceIdAndAllTracesStream = spansWindowedStream
        .map((KeyValueMapper<Windowed<String>, Trace, KeyValue<String, Trace>>) (key,
            value) -> new KeyValue<>(key.key(), value));

    // TODO Ordering in Trace
    // TODO implement count attribute in Trace -> number of similar traces
    // TODO Reduce traceIdAndAllTracesStream to similiar traces stream (map and reduce)
    // use hash for trace https://docs.confluent.io/current/streams/quickstart.html#purpose

    traceIdAndAllTracesStream.to(OUT_TOPIC);


    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}

