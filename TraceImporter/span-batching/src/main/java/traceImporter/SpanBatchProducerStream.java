package traceImporter;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanBatchProducerStream {

  private static final String IN_TOPIC = "cluster-spans";
  private static final String OUT_TOPIC = "span-batches";

  private final Properties streamsConfig = new Properties();

  public SpanBatchProducerStream() {

    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "span-batching");

    streamsConfig.put("schema.registry.url", "http://localhost:8081");

    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

  }


  public void run() {
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, EVSpan> evSpanStream = builder.stream(IN_TOPIC);

    TimeWindowedKStream<String, EVSpan> windowedStream =
        evSpanStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofSeconds(10)));

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

    KStream<Windowed<String>, EVSpanList> evSpanWindowedStream =
        messagesAggregatedByWindow.toStream();

    evSpanWindowedStream.foreach(new ForeachAction<Windowed<String>, EVSpanList>() {
      public void apply(Windowed<String> key, EVSpanList value) {
        System.out.println(key.key() + ": " + value.getSpanList().get(0).getTraceId() + "; "
            + value.getSpanList().size());
      }
    });

    KStream<String, EVSpanList> transformed = evSpanWindowedStream
        .map(new KeyValueMapper<Windowed<String>, EVSpanList, KeyValue<String, EVSpanList>>() {
          @Override
          public KeyValue<String, EVSpanList> apply(Windowed<String> key, EVSpanList value) {
            return new KeyValue<>(key.key(), value);
          }
        });


    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    // Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer, stringDeserializer);

    TimeWindowedSerializer<String> windowedSerializer =
        new TimeWindowedSerializer<String>(stringSerializer);
    TimeWindowedDeserializer<String> windowedDeserializer =
        new TimeWindowedDeserializer<String>(stringDeserializer);

    Serde<Windowed<String>> windowedSerde =
        Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

    SpecificAvroSerde<EVSpanList> listSerializer = new SpecificAvroSerde<EVSpanList>();

    transformed.to(OUT_TOPIC);
    // evSpanWindowedStream.to(OUT_TOPIC);

    // Serializer<List<String>> listSerializer = new ListSerializer<String>(stringSerializer);

    // https://github.com/apache/kafka/pull/6592

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

