package traceImporter;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.WindowStore;
import traceImporter.serdes.LinkedListSerde;

import java.time.Duration;
import java.util.*;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 *  topic 'span-batches'
 */
public class SpanBatchProducer implements Runnable {

    private static final String TOPIC = "cluster-spans";

    private final Properties properties = new Properties();

    public SpanBatchProducer() {

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "explorviz");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

        properties.put("bootstrap.servers", "localhost:9091");
        properties.put("group.id", "trace-importer-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

    }


    @Override
    public void run() {
        TimeWindows w = TimeWindows.of(Duration.ofSeconds(10));
        StreamsBuilder builder = new StreamsBuilder();


        KStream<Long, byte[]> spans = builder.stream(TOPIC);


        KTable<Windowed<Long>, LinkedList<byte[]>> table =
            spans.filter((k, v) -> k != null && v != null)
                .groupByKey()
                .windowedBy(w)
                .aggregate(LinkedList::new, (key, value, aggregate) -> {
                    aggregate.add(value);
                    return aggregate;
                }, Materialized.with(Serdes.Long(), new LinkedListSerde<>(Serdes.ByteArray())));

        table.toStream().to("span-batches",
            Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class),
                new LinkedListSerde<>(Serdes.ByteArray())));


        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }



}

