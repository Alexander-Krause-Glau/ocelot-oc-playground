package traceImporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.trace.v1.Span;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import traceImporter.serdes.JsonDeserializer;
import traceImporter.serdes.JsonSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 *  topic 'span-batches'
 */
public class SpanBatchProducer implements Runnable {

    private static final String TOPIC = "cluster-spans";

    private final Properties properties = new Properties();

    public SpanBatchProducer() {

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "explorviz");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        properties.put("bootstrap.servers", "localhost:9091");
        properties.put("group.id", "trace-importer-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", LongDeserializer.class);// NOCS
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }


    @Override
    public void run() {
        TimeWindows w = TimeWindows.of(Duration.ofSeconds(10l));
        StreamsBuilder builder = new StreamsBuilder();

        Serde<LinkedList<Span>> jsonSerde = new Serde<LinkedList<Span>>() {
            @Override
            public Serializer<LinkedList<Span>> serializer() {
                return new JsonSerializer<LinkedList<Span>>();
            }

            @Override
            public Deserializer<LinkedList<Span>> deserializer() {
                Deserializer<LinkedList<Span>> des =  new JsonDeserializer<>();
                Map<String, Class> map = new HashMap<>();
                map.put("JsonPOJOClass", LinkedList.class);
                des.configure(map, false);
                return des;
            }
        };

        KStream<Long, byte[]> spans = builder.stream(TOPIC);
        KTable<Windowed<Long>, LinkedList<Span>> table = spans
                .filter((k, v) -> k != null && v != null)
                .mapValues(value -> {
                    try {
                        return Span.parseFrom(value);
                    } catch (InvalidProtocolBufferException e) {
                        return null;
                    }
                })
                .groupByKey()
                .windowedBy(w)
                .aggregate(() -> new LinkedList<>(), (key, value, aggregate) -> {
                    aggregate.add(value);
                    return aggregate;
                }, Materialized.with(Serdes.Long(), jsonSerde));
        table.toStream().to("span-batches", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class), jsonSerde));


        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }







}

