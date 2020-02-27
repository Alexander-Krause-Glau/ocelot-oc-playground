package traceImporter;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.*;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanToTraceReconstructorStream {


  private final Properties streamsConfig = new Properties();

  private final Topology topology;

  private final SchemaRegistryClient registryClient;

  public SpanToTraceReconstructorStream(final SchemaRegistryClient schemaRegistryClient) {

    this.streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER);
    this.streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KafkaConfig.COMMIT_INTERVAL_MS);
    this.streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        KafkaConfig.TIMESTAMP_EXTRACTOR);
    this.streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APP_ID);

    this.registryClient = schemaRegistryClient;

    this.topology = this.buildTopology();
  }


  public Topology getTopology() {
    return this.topology;
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, EVSpan> explSpanStream = builder.stream(KafkaConfig.IN_TOPIC,
        Consumed.with(Serdes.String(), this.getAvroSerde(false)));

    // Window spans in 4s intervals with 2s grace period
    final TimeWindowedKStream<String, EVSpan> windowedEvStream = explSpanStream.groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofSeconds(4)).grace(Duration.ofSeconds(2)));

    // Aggregate Spans to traces and deduplicate similar spans of a trace
    final KTable<Windowed<String>, Trace> traceTable =
        windowedEvStream.aggregate(Trace::new, (traceId, evSpan, trace) -> {

          // Initialize Span according to first span of the trace
          final long evSpanStartTime = evSpan.getStartTime();
          final long evSpanEndTime = evSpan.getEndTime();
          if (trace.getSpanList() == null) {
            trace.setSpanList(new ArrayList<>());
            trace.getSpanList().add(evSpan);

            trace.setStartTime(evSpanStartTime);
            trace.setEndTime(evSpanEndTime);
            trace.setOverallRequestCount(1);
            trace.setDuration(evSpanEndTime - evSpanStartTime);

            trace.setTraceCount(1);

            // set initial trace id - do not change, since this is the major key for kafka
            // partitioning
            trace.setTraceId(evSpan.getTraceId());
          } else {

            // TODO
            // Implement
            // - traceDuration
            // - Tracesteps with caller callee each = EVSpan


            // Find duplicates in Trace (via fqn), aggregate based on request count
            // Furthermore, potentially update trace values
            trace.getSpanList().stream()
                .filter(s -> s.getOperationName().contentEquals(evSpan.getOperationName()))
                .findAny().ifPresentOrElse(s -> {
                  s.setRequestCount(s.getRequestCount() + 1);
                  s.setStartTime(Math.min(s.getStartTime(), evSpan.getStartTime()));
                  s.setEndTime(Math.max(s.getEndTime(), evSpan.getEndTime()));
                }, () -> trace.getSpanList().add(evSpan));
            trace.getSpanList().stream().mapToLong(EVSpan::getStartTime).min()
                .ifPresent(trace::setStartTime);
            trace.getSpanList().stream().mapToLong(EVSpan::getEndTime).max()
                .ifPresent(trace::setEndTime);
            trace.setDuration(trace.getEndTime() - trace.getStartTime());


          }
          return trace;
        }, Materialized.with(Serdes.String(), this.getAvroSerde(false)));

    final KStream<Windowed<String>, Trace> traceStream = traceTable.toStream();

    // Map traces to a new key that resembles all included spans

    final KStream<Windowed<EVSpanKey>, Trace> traceIdSpanStream = traceStream.flatMap((key, trace) -> {

      System.out.println("key: " + key.window().startTime());

      final List<KeyValue<Windowed<EVSpanKey>, Trace>> result = new LinkedList<>();

      final List<EVSpanData> spanDataList = new ArrayList<>();

      for (final EVSpan span : trace.getSpanList()) {
        spanDataList
            .add(new EVSpanData(span.getOperationName(), span.getHostname(), span.getAppName()));
      }

      final EVSpanKey newKey = new EVSpanKey(spanDataList);

      final Windowed<EVSpanKey> newWindowedKey = new Windowed<>(newKey, key.window());

      result.add(KeyValue.pair(newWindowedKey, trace));
      return result;
    });


    // Reduce similar Traces of one window to a single Trace
    final KTable<Windowed<EVSpanKey>, Trace> reducedTraceTable = traceIdSpanStream
        .groupByKey(Grouped.with(getWindowedAvroSerde(), getAvroSerde(false)))
        .aggregate(Trace::new, (sharedTraceKey, trace, reducedTrace) -> {


          if (reducedTrace.getTraceId() == null) {
            reducedTrace = trace;
          } else {
            reducedTrace.setTraceCount(reducedTrace.getTraceCount() + 1);
            // Use the Span list of the latest trace in the group
            // Do so since span list only grow but never loose elements
            reducedTrace.setSpanList(trace.getSpanList());

            // Update start and end time of the trace

            reducedTrace.setStartTime(Math.min(trace.getStartTime(), reducedTrace.getStartTime()));
            reducedTrace.setEndTime(Math.max(trace.getEndTime(), reducedTrace.getEndTime()));
          }

          return reducedTrace;
        }, Materialized.with(this.getWindowedAvroSerde(), this.getAvroSerde(false)));
    // .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

    final KStream<Windowed<EVSpanKey>, Trace> reducedTraceStream = reducedTraceTable.toStream();

    final KStream<String, Trace> reducedIdTraceStream = reducedTraceStream.flatMap((key, value) -> {

      final List<KeyValue<String, Trace>> result = new LinkedList<>();

      result.add(KeyValue.pair(value.getTraceId(), value));
      return result;
    });

    reducedIdTraceStream.foreach((key, trace) -> {

      final List<EVSpan> list = trace.getSpanList();
      System.out.println("Trace with id " + trace.getTraceId());
      list.forEach((val) -> {
        System.out.println(val.getStartTime() + " : " + val.getEndTime() + " für "
            + val.getOperationName() + " mit Anzahl " + val.getRequestCount());
      });

    });


    // Sort spans in each trace based of start time
    reducedIdTraceStream.peek(
        (key, trace) -> trace.getSpanList().sort(Comparator.comparingLong(EVSpan::getStartTime)));

    // TODO implement count attribute in Trace -> number of similar traces
    // TODO Reduce traceIdAndAllTracesStream to similiar traces stream (map and reduce)
    // use something like hash for trace
    // https://docs.confluent.io/current/streams/quickstart.html#purpose

    reducedIdTraceStream.to(KafkaConfig.OUT_TOPIC,
        Produced.with(Serdes.String(), this.getAvroSerde(false)));
    return builder.build();
  }

  public void run() {

    final KafkaStreams streams = new KafkaStreams(this.topology, this.streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  /**
   * Creates a {@link Serde} for specific avro records using the {@link SpecificAvroSerde}
   * @param forKey {@code true} if the Serde is for keys, {@code false} otherwise
   * @param <T> type of the avro record
   * @return a Serde
   */
  private <T extends SpecificRecord> SpecificAvroSerde<T> getAvroSerde(final boolean forKey) {
    final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde<>(this.registryClient);
    valueSerde.configure(
        Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConfig.REGISTRY_URL),
        forKey);

    return valueSerde;
  }

  /**
   * Creates a new Serde for windowed keys of specific avro records
   * @param <T> avro record data type
   * @return a {@link Serde} for specific avro records wrapped in a time window
   */
  private <T extends SpecificRecord> Serde<Windowed<T>> getWindowedAvroSerde() {
    Serde<T> valueSerde = getAvroSerde(true);
    TimeWindowedSerializer<T> ser = new TimeWindowedSerializer<T>(valueSerde.serializer());
    TimeWindowedDeserializer<T> de = new TimeWindowedDeserializer<T>(valueSerde.deserializer());
    return Serdes.serdeFrom(ser, de);
  }


}

