package traceImporter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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

    KTable<String, Trace> traceTable =
        explSpanStream.groupByKey().aggregate(Trace::new, (traceId, evSpan, trace) -> {

          long evSpanStartTime = evSpan.getStartTime();
          long evSpanEndTime = evSpan.getEndTime();

          if (trace.getSpanList() == null) {
            trace.setSpanList(new ArrayList<>());
            trace.getSpanList().add(evSpan);

            trace.setStartTime(evSpanStartTime);
            trace.setEndTime(evSpanEndTime);
            trace.setOverallRequestCount(1);
            trace.setDuration(evSpanEndTime - evSpanStartTime);

            trace.setTraceCount(1);

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

                // Take min startTime of similar spans for the span representative
                if (includedSpan.getStartTime() > evSpanStartTime) {
                  // System.out.println(
                  // "Updated from start " + trace.getStartTime() + " to " + evSpanStartTime);
                  includedSpan.setStartTime(evSpanStartTime);

                }

                // Take max endTime of similar spans for the span representative
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

    KStream<String, Trace> traceStream = traceTable.toStream();

    // TODO A serializer (key: org.apache.kafka.common.serialization.StringSerializer / value:
    // io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer) is not compatible to the
    // actual key or value type (key type:
    // traceImporter.SpanToTraceReconstructorStream$SharedTraceData / value type:
    // traceImporter.Trace). Change the default Serdes in StreamConfig or provide correct Serdes via
    // method parameters.

    KStream<EVSpanKey, Trace> traceIdSpanStream = traceStream.flatMap((key, value) -> {

      List<KeyValue<EVSpanKey, Trace>> result = new LinkedList<>();

      List<EVSpanData> spanDataList = new ArrayList<>();

      for (EVSpan span : value.getSpanList()) {
        spanDataList
            .add(new EVSpanData(span.getOperationName(), span.getHostname(), span.getAppName()));
      }

      EVSpanKey newKey = new EVSpanKey(spanDataList);

      result.add(KeyValue.pair(newKey, value));
      return result;
    });

    // traceIdSpanStream.foreach((key, value) -> {
    // System.out.printf("New trace with %d spans (id: %s)\n", value.getSpanList().size(),
    // key.getSpanList().get(0).getOperationName());
    // });

    final Map<String, String> serdeConfig =
        Collections.singletonMap("schema.registry.url", "http://localhost:8081");
    // `Foo` and `Bar` are Java classes generated from Avro schemas
    final Serde<EVSpanKey> keySpecificAvroSerde = new SpecificAvroSerde<>();
    keySpecificAvroSerde.configure(serdeConfig, true); // `true` for record keys
    final Serde<Trace> valueSpecificAvroSerde = new SpecificAvroSerde<>();
    valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values

    TimeWindowedKStream<EVSpanKey, Trace> windowedStream =
        traceIdSpanStream.groupByKey(Grouped.with(keySpecificAvroSerde, valueSpecificAvroSerde))
            .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).grace(Duration.ofSeconds(2)));

    KTable<Windowed<EVSpanKey>, Trace> reducedTraceTable =
        windowedStream.aggregate(Trace::new, (sharedTraceKey, trace, reducedTrace) -> {

          // TODO build reduced trace here

          reducedTrace.setTraceId(trace.getTraceId());
          reducedTrace.setTraceCount(reducedTrace.getTraceCount() + 1);

          if (reducedTrace.getSpanList() == null) {
            reducedTrace.setSpanList(trace.getSpanList());
          }

          return reducedTrace;
        }, Materialized.with(keySpecificAvroSerde, valueSpecificAvroSerde));

    KStream<Windowed<EVSpanKey>, Trace> reducedTraceStream = reducedTraceTable.toStream();

    KStream<String, Trace> reducedIdTraceStream = reducedTraceStream.flatMap((key, value) -> {

      List<KeyValue<String, Trace>> result = new LinkedList<>();

      result.add(KeyValue.pair(value.getTraceId(), value));
      return result;
    });

    reducedIdTraceStream.foreach((key, value) -> {

      System.out.printf("New trace with %d spans (id: %s, traceCount: %d)\n",
          value.getSpanList().size(), key, value.getTraceCount());

      List<EVSpan> list = value.getSpanList();

      list.forEach((val) -> {
        System.out.println(val.getStartTime() + " : " + val.getEndTime() + " für "
            + val.getOperationName() + " mit Anzahl " + val.getRequestCount());
      });

    });

    // spansWindowedStream.foreach((key, value) -> System.out
    // .printf("New trace with %d spans (id: %s)\n", value.getSpanList().size(), key));

    // KStream<String, Trace> traceIdAndReducedTracesStream = reducedTraceStream
    // .map((KeyValueMapper<Windowed<EVSpanKey>, Trace, KeyValue<String, Trace>>) (key,
    // value) -> new KeyValue<>(value.getTraceId(), value));


    // TODO Ordering in Trace
    // TODO implement count attribute in Trace -> number of similar traces
    // TODO Reduce traceIdAndAllTracesStream to similiar traces stream (map and reduce)
    // use hash for trace https://docs.confluent.io/current/streams/quickstart.html#purpose

    reducedIdTraceStream.to(OUT_TOPIC);
    // traceStream.to(OUT_TOPIC);


    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public EVSpanKey createEVSpanKeyForTrace(Trace t) {
    List<EVSpanData> spanDataList = new ArrayList<>();

    for (EVSpan span : t.getSpanList()) {
      spanDataList
          .add(new EVSpanData(span.getOperationName(), span.getHostname(), span.getAppName()));
    }

    return new EVSpanKey(spanDataList);
  }

}

