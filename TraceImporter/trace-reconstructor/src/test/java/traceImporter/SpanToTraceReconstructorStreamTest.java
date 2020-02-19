package traceImporter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SpanToTraceReconstructorStreamTest {


  private TopologyTestDriver testDriver;
  private TestInputTopic<String, EVSpan> inputTopic;
  private TestOutputTopic<String, Trace> outputTopic;

  @BeforeEach
  void setUp() throws IOException, RestClientException {

    final MockSchemaRegistryClient mockSRC = new MockSchemaRegistryClient();
    mockSRC.register(KafkaConfig.OUT_TOPIC + "-value", Trace.SCHEMA$);
    mockSRC.register(KafkaConfig.IN_TOPIC + "-key", EVSpanKey.SCHEMA$);
    mockSRC.register(KafkaConfig.IN_TOPIC + "-value", EVSpan.SCHEMA$);
    // mockSRC.register("STREAM-AGGREGATE-STATE-STORE-0000000005-repartition-key",
    // EVSpanKey.SCHEMA$);

    final Topology topo = new SpanToTraceReconstructorStream(mockSRC).getTopology();
    System.out.println(topo.describe().toString());

    final Serializer<EVSpan> evSpanSerializer = new SpecificAvroSerde<EVSpan>(mockSRC).serializer();
    final Deserializer<Trace> traceDeserializer =
        new SpecificAvroSerde<Trace>(mockSRC).deserializer();

    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        KafkaConfig.TIMESTAMP_EXTRACTOR);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

    final Map<String, String> conf =
        Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy");
    evSpanSerializer.configure(conf, false);

    traceDeserializer.configure(conf, false);

    this.testDriver = new TopologyTestDriver(topo, props);

    this.inputTopic = this.testDriver.createInputTopic(KafkaConfig.IN_TOPIC,
        Serdes.String().serializer(), evSpanSerializer);
    this.outputTopic = this.testDriver.createOutputTopic(KafkaConfig.OUT_TOPIC,
        Serdes.String().deserializer(), traceDeserializer);

    mockSRC.getAllSubjects().forEach(System.out::println);
  }

  @AfterEach
  void afterEach() {
    this.testDriver.close();
  }


  /**
   * Tests whether multiple spans with the same operation name belonging to the same trace are
   * reduced to a single span with an updated {@link EVSpan#requestCount}
   */
  @Test
  void testSpanDeduplication() {

    final String traceId = "testtraceid";
    final String operationName = "OpName";

    final long start1 = 10L;
    final long end1 = 20L;

    final long start2 = 40L;
    final long end2 = 80L;


    final EVSpan evSpan1 = new EVSpan("1", traceId, start1, end1, end1 - start1, operationName, 1,
        "samplehost", "sampleapp");
    final EVSpan evSpan2 = new EVSpan("2", traceId, start2, end2, end2 - start2, operationName, 1,
        "samplehost", "sampleapp");


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);


    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    assertEquals(2, records.size());

    assertEquals(traceId, records.get(0).key);
    assertEquals(traceId, records.get(1).key);

    // Trace is "completed" after two updates, thus take the second record
    final Trace trace = records.get(1).value;

    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());

    // Deduplication
    assertEquals(1, trace.getSpanList().size());
    assertEquals(2, trace.getSpanList().get(0).getRequestCount());

  }


  /**
   * Tests if a trace's span list is sorted w.r.t. to the start time of each span
   */
  @Test
  void testOrdering() {

    final String traceId = "testtraceid";

    final EVSpan evSpan1 =
        new EVSpan("1", traceId, 10L, 20L, 10L, "OpB", 1, "samplehost", "sampleapp");
    final EVSpan evSpan2 =
        new EVSpan("2", traceId, 5L, 10L, 5L, "OpA", 1, "samplehost", "sampleapp");

    // This Span's timestamp is 7 seconds later than the first thus closing the window containing
    // the first two spans
    final EVSpan windowTerminationSpan = new EVSpan("212", "sometrace",
        Duration.ofSeconds(7).toNanos(), 1L, 2L, "SomeOperation", 1, "samplehost", "sampleapp");


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);
    this.inputTopic.pipeInput(windowTerminationSpan.getTraceId(), windowTerminationSpan);


    assertEquals(3, this.outputTopic.getQueueSize());
    final Trace trace = this.outputTopic.readKeyValuesToList().get(1).value;

    // Trace must contain both spans
    assertEquals(2, trace.getSpanList().size());

    // Spans in span list must be sorted by start time
    assertTrue(
        trace.getSpanList().get(0).getStartTime() < trace.getSpanList().get(1).getStartTime());

  }


  /**
   * Tests whether a correct trace is generated based on only a single span
   */
  @Test
  void testTraceCreation() {
    final String traceId = "testtraceid";

    final EVSpan span =
        new EVSpan("1", traceId, 10L, 20L, 10L, "OpB", 1, "samplehost", "sampleapp");
    this.inputTopic.pipeInput(span.getTraceId(), span);

    final Trace trace = this.outputTopic.readValue();
    assertNotNull(trace);

    assertEquals(traceId, trace.getTraceId());
    assertEquals(span.getDuration(), trace.getDuration());
    assertEquals(span.getStartTime(), trace.getStartTime());
    assertEquals(span.getEndTime(), trace.getEndTime());
    assertEquals(1, trace.getTraceCount());
    assertEquals(1, trace.getSpanList().size());

  }


  /**
   * Tests the windowing of traces. Spans with the same trace id in close temporal proximity should
   * be aggregated in the same trace. If another span with the same trace id arrives later, it
   * should not be included in the same trace.
   */
  @Test
  void testWindowing() {
    final String traceId = "testtraceid";

    final long start1 = System.currentTimeMillis();
    final long end1 = System.currentTimeMillis();

    final long start2 = end1 + 100L;
    final long end2 = start2 + 100L;

    final long start3 = start1 + Duration.ofSeconds(50).toMillis();
    final long end3 = start3 + 5000L;



    final EVSpan evSpan1 =
        new EVSpan("1", traceId, start1, end1, end1 - start1, "OpA", 1, "samplehost", "sampleapp");
    final EVSpan evSpan2 =
        new EVSpan("2", traceId, start2, end2, end2 - start2, "OpB", 1, "samplehost", "sampleapp");

    // This Span's timestamp is 7 seconds later than the first thus closing the window containing
    // the first two spans
    final EVSpan evSpan3 = new EVSpan("212", traceId, start3, end3, end3 - start3, "OpC", 1,
        "samplehost", "sampleapp");


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1, start1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2, start2);
    this.inputTopic.pipeInput(evSpan3.getTraceId(), evSpan3, start3);


    // First two Spans should be in a window. However, a record should be created for each update.
    assertEquals(3, this.outputTopic.getQueueSize());

    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    // First Trace should encompass first two spans
    final Trace trace = records.get(1).value;
    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());
    assertEquals(1, trace.getTraceCount());
    assertEquals(2, trace.getSpanList().size());

    // Second trace should only include the last span
    final Trace trace2 = records.get(2).value;
    System.out.println(trace2);
    assertEquals(start3, trace2.getStartTime());
    assertEquals(end3, trace2.getEndTime());
    assertEquals(1, trace2.getTraceCount());
    assertEquals(1, trace2.getSpanList().size());
  }



  /**
   * Spans with different trace id that are otherwise similar, should be reduced to a single trace
   */
  @Test
  void testTraceReduction() {
    final String operationName = "OpName";

    final long start1 = 10L;
    final long end1 = 20L;

    final long start2 = 40L;
    final long end2 = 80L;


    final EVSpan evSpan1 = new EVSpan("1", "trace1", start1, end1, end1 - start1, operationName, 1,
        "samplehost", "sampleapp");
    final EVSpan evSpan2 = new EVSpan("2", "trace2", start2, end2, end2 - start2, operationName,
        265, "samplehost", "sampleapp");


    this.inputTopic.pipeInput(evSpan1.getTraceId(), evSpan1);
    this.inputTopic.pipeInput(evSpan2.getTraceId(), evSpan2);


    final List<KeyValue<String, Trace>> records = this.outputTopic.readKeyValuesToList();

    assertEquals(2, records.size());

    // Trace is "completed" after two updates, thus take the second record
    final Trace trace = records.get(1).value;

    assertEquals(start1, trace.getStartTime());
    assertEquals(end2, trace.getEndTime());

    // Reduction
    assertEquals(2, trace.getTraceCount());

    // Only the span list of the latest trace in the reduction should be used
    assertEquals(1, trace.getSpanList().size());
    assertEquals(265, trace.getSpanList().get(0).getRequestCount());

  }

}
