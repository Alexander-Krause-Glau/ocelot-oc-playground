package traceImporter;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opencensus.proto.dump.DumpSpans;
import io.opencensus.proto.trace.v1.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import traceImporter.serdes.JsonDeserializer;

import java.time.Duration;
import java.util.*;


public class SpanBatchConsumer implements Runnable{

    private static final String TOPIC = "span-batches";
    private static final Logger LOGGER = LoggerFactory.getLogger(SpanBatchConsumer.class);
    KafkaConsumer<Windowed<Long>, List<Span>> consumer;

    public SpanBatchConsumer() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9091");
        properties.put("group.id", "trace-importer-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");

        Deserializer<List<Span>> des =  new JsonDeserializer<>();
        Map<String, Class> map = new HashMap<>();
        map.put("JsonPOJOClass", LinkedList.class);
        des.configure(map, false);

        this.consumer = new KafkaConsumer<>(properties, new TimeWindowedDeserializer<Long>(new LongDeserializer()), des);
    }


    @Override
    public void run() {
        this.consumer.subscribe(Arrays.asList(TOPIC));


        while (true) {
            final ConsumerRecords<Windowed<Long>, List<Span>> records =
                    this.consumer.poll(Duration.ofMillis(100));

            for (final ConsumerRecord<Windowed<Long>, List<Span>> record : records) {
                //try {
                    System.out.println("CLAZZ = " + record.value().get(0).getClass().toString());
                    //Span s =  Span.parseFrom(record.value().get(0));
                    //LOGGER.info("New batch with {} spans of trace with id {}", record.value().size(), s.getTraceId());
                //} //catch (InvalidProtocolBufferException e) {
                    //e.printStackTrace();
                //}


            }
        }
    }
}
