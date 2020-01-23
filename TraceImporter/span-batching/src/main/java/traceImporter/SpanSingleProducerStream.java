package traceImporter;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanSingleProducerStream {

  private static final String IN_TOPIC = "cluster-spans";
  private static final String OUT_TOPIC = "span-batches";

  private final Properties streamsConfig = new Properties();


  public SpanSingleProducerStream() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "span-batching");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

  }



  public void run() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, EVSpan> evSpanStream = builder.stream(IN_TOPIC);

    evSpanStream.foreach(new ForeachAction<String, EVSpan>() {
      public void apply(String key, EVSpan value) {
        System.out.println(key + ": " + value.getSpanId());
      }
    });

    System.out.println("test");

    evSpanStream.to(OUT_TOPIC);

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

