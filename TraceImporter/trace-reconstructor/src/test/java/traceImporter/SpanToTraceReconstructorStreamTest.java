package traceImporter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanToTraceReconstructorStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, EVSpan> inputTopic;
  private TestOutputTopic<String, Trace> outputTopic;

  @BeforeEach
  void setUp() throws IOException, RestClientException {

    MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();
    mockSRC.register(KafkaConfig.OUT_TOPIC + "-value", Trace.SCHEMA$);
    mockSRC.register(KafkaConfig.IN_TOPIC + "-key", EVSpanKey.SCHEMA$);
    mockSRC.register(KafkaConfig.IN_TOPIC + "-value", EVSpan.SCHEMA$);
    // mockSRC.register("STREAM-AGGREGATE-STATE-STORE-0000000005-repartition-key",
    // EVSpanKey.SCHEMA$);

    Topology topo = (new SpanToTraceReconstructorStream(mockSRC)).getTopology();
    System.out.println(topo.describe().toString());

    final Serializer<EVSpan> evSpanSerializer = new SpecificAvroSerde<EVSpan>(mockSRC).serializer();
    final Deserializer<Trace> traceDeserializer =
        new SpecificAvroSerde<Trace>(mockSRC).deserializer();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        KafkaConfig.TIMESTAMP_EXTRACTOR);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    Map<String, String> conf =
        Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy");
    evSpanSerializer.configure(conf, false);

    traceDeserializer.configure(conf, false);

    testDriver = new TopologyTestDriver(topo, props);

    inputTopic = testDriver.createInputTopic(KafkaConfig.IN_TOPIC, Serdes.String().serializer(),
        evSpanSerializer);
    outputTopic = testDriver.createOutputTopic(KafkaConfig.OUT_TOPIC,
        Serdes.String().deserializer(), traceDeserializer);

    mockSRC.getAllSubjects().forEach(System.out::println);
  }

  @AfterEach
  void afterEach() {
    testDriver.close();
  }


  @Test
  void testWindowing() throws InterruptedException {
    // Push two spans with the same trace id and operation name to the input topic with a gap of 5
    // seconds

    final String traceId = "testtraceid";
    final String operationName = "OpName";

    long start1 = Duration.ofDays(128).toNanos();
    long end1 = start1 + Duration.ofMillis(20).toNanos();

    long start2 = start1 + Duration.ofMillis(1).toNanos();
    long end2 = start2 + Duration.ofMillis(120).toNanos();



    EVSpan evSpan1 = new EVSpan("1", traceId, start1, end1, end1 - start1, operationName, 1,
        "samplehost", "sampleapp");
    EVSpan evSpan2 = new EVSpan("2", traceId, start2, end2, end2 - start2, operationName, 1,
        "samplehost", "sampleapp");

    // This Span's timestamp is 7 seconds later than the first thus closing the window containing
    // the first two spans
    EVSpan windowTerminationSpan =
        new EVSpan("212", "sometrace", start1 + Duration.ofSeconds(4 + 3).toNanos(), 1L, 2L,
            "SomeOperation", 1, "samplehost", "sampleapp");


    inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);
    inputTopic.pipeInput(windowTerminationSpan.getTraceId(), windowTerminationSpan);


    assertEquals(3, outputTopic.getQueueSize());

    List<KeyValue<String, Trace>> records = outputTopic.readKeyValuesToList();

    // First two Spans should be in a window. However, a record should be created for each update.

    Assertions.assertEquals(traceId, records.get(0).key);
    Assertions.assertEquals(traceId, records.get(1).key);

    Trace firstUpdate = records.get(0).value;
    System.out.println(firstUpdate);
    Trace secondsUpdate = records.get(1).value;
    System.out.println(secondsUpdate);

    assertEquals(start1, firstUpdate.getStartTime());
    assertEquals(end1, firstUpdate.getEndTime());
    assertEquals(1, firstUpdate.getTraceCount());

    assertEquals(start1, secondsUpdate.getStartTime());
    assertEquals(end2, secondsUpdate.getEndTime());
    assertEquals(2, secondsUpdate.getTraceCount());
    assertEquals(2, secondsUpdate.getSpanList().get(0).getRequestCount());

  }

}
