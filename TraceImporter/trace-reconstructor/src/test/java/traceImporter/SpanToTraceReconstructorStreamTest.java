package traceImporter;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

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
  void testWindowExceedingTrace() {
    // Push two spans with the same trace id and operation name to the input topic with a gap of 5
    // seconds

    final String traceId = "testtraceid";
    final String operationName = "OpName";
    long startTime = System.nanoTime();
    long endTime = System.nanoTime() + Duration.ofSeconds(1).toNanos();
    long duration = endTime - startTime;

    EVSpan evSpan1 = new EVSpan("1", traceId, startTime, endTime, duration, operationName, 2,
        "samplehost", "sampleapp");
    EVSpan evSpan2 = new EVSpan("2", traceId, startTime, endTime, duration, operationName, 1,
        "samplehost", "sampleapp");

    inputTopic.pipeInput(traceId, evSpan1);
    testDriver.advanceWallClockTime(Duration.ofSeconds(1));
    inputTopic.pipeInput(traceId, evSpan2);

    // Must hold true since a new trace is generated each 4 seconds
    Assertions.assertEquals(2L, outputTopic.getQueueSize());

    outputTopic.readKeyValuesToList().forEach((e) -> {
      System.out.println(e);
    });


  }

}
