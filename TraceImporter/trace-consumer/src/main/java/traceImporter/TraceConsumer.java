package traceImporter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Consumes {@link Trace} objects.
 */
public class TraceConsumer {

  private static final String IN_TOPIC = "explorviz-traces";
  private static final String OUT_TOPIC = "nothing-at-the-moment";

  private final Properties streamsConfig = new Properties();

  public TraceConsumer() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "trace-consuming");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
  }


  public void run() {
    StreamsBuilder builder = new StreamsBuilder();

    // Stream 1

    KStream<String, Trace> traceIdTraceStream = builder.stream(IN_TOPIC);

    traceIdTraceStream.foreach((key, value) -> {

      System.out.printf("New trace with %d spans (id: %s, traceCount: %d)\n",
          value.getSpanList().size(), key, value.getTraceCount());

      List<EVSpan> list = value.getSpanList();

      list.forEach((val) -> {
        System.out.println(val.getStartTime() + " : " + val.getEndTime() + " für "
            + val.getOperationName() + " mit Anzahl " + val.getRequestCount());
      });

    });

    // traceIdTraceStream.to(OUT_TOPIC);
    final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}

