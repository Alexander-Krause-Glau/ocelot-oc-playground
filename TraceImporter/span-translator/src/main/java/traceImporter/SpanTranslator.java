package traceImporter;

import com.google.common.io.BaseEncoding;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.AttributeValue;
import io.opencensus.proto.trace.v1.Span;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Translates opencensus {@link Span} objects to {@link EVSpan}s.
 */
public class SpanTranslator {



    private final Properties streamsConfig = new Properties();

    private Topology topology;

    private final SchemaRegistryClient registry;

    public SpanTranslator(SchemaRegistryClient registry) {

        this.registry = registry;
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BROKER);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APPLICATION_ID);

        buildTopology();
    }

    public Topology getTopology() {
        return topology;
    }


    private void buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Stream 1

        KStream<byte[], byte[]> dumpSpanStream =
            builder.stream(KafkaConfig.IN_TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

        KStream<String, EVSpan> traceIdSpanStream = dumpSpanStream.flatMap((key, value) -> {

            DumpSpans dumpSpan;
            List<KeyValue<String, EVSpan>> result = new LinkedList<>();
            try {

                dumpSpan = DumpSpans.parseFrom(value);

                for (Span s : dumpSpan.getSpansList()) {
                    String traceId = BaseEncoding.base16().lowerCase()
                        .encode(s.getTraceId().toByteArray(), 0, 16);

                    String spanId =
                        BaseEncoding.base16().lowerCase().encode(s.getSpanId().toByteArray(), 0, 8);

                    long startTime = Duration
                        .ofSeconds(s.getStartTime().getSeconds(), s.getStartTime().getNanos())
                        .toNanos();

                    long endTime =
                        Duration.ofSeconds(s.getEndTime().getSeconds(), s.getEndTime().getNanos())
                            .toNanos();
                    long duration = endTime - startTime;



                    // System.out.println(startTime + " und " + s.getStartTime().getSeconds());

                    // System.out.println(Duration
                    // .ofNanos(Duration
                    // .ofSeconds(s.getStartTime().getSeconds(), s.getStartTime().getNanos()).toNanos())
                    // .toMillis());

                    // System.out.println(duration + " und " + Duration.between(t, t1).getNano());

                    Map<String, AttributeValue> attributes = s.getAttributes().getAttributeMapMap();
                    String operationName = attributes.get("method_fqn").getStringValue().getValue();
                    String hostname = attributes.get("host").getStringValue().getValue();
                    String appName = attributes.get("application_name").getStringValue().getValue();


                    EVSpan span =
                        new EVSpan(spanId, traceId, startTime, endTime, duration, operationName, 1,
                            hostname, appName);

                    result.add(KeyValue.pair(traceId, span));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return result;


        });

        traceIdSpanStream.to(KafkaConfig.OUT_TOPIC, Produced.with(Serdes.String(), getValueSerde()));

        this.topology = builder.build();
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde() {
        final SpecificAvroSerde<T> valueSerde = new SpecificAvroSerde<>(registry);
        valueSerde.configure(
            Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
            false);
        return valueSerde;
    }

    public void run() {

        final KafkaStreams streams = new KafkaStreams(this.topology, streamsConfig);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

