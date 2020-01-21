package traceImporter;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Collects spans for 10 seconds, grouped by the trace id, and forwards the resulting batch to the
 * topic 'span-batches'
 */
public class SpanBatchProducer implements Runnable {

  private static final String IN_TOPIC = "cluster-spans";
  private static final String OUT_TOPIC = "span-batches";


  private final Properties properties = new Properties();


  private KafkaProducer<Long, List<EVSpan>> kafkaProducer;
  private KafkaConsumer<Long, EVSpan> kafkaConsumer;


  public SpanBatchProducer() {


    properties.put("bootstrap.servers", "localhost:9091");
    properties.put("group.id", "trace-importer-1");
    properties.put("enable.auto.commit", "true");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("acks", "all");
    properties.put("retries", "1");
    properties.put("batch.size", "16384");
    properties.put("linger.ms", "1");
    properties.put("max.request.size", "2097152");
    properties.put("buffer.memory", 33_554_432);
    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:8081");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);


    Serializer<List<EVSpan>> listSerializer = new ListSerializer(new KafkaAvroSerializer());

    kafkaProducer = new KafkaProducer<>(properties, new LongSerializer(), listSerializer);

    kafkaConsumer = new KafkaConsumer<>(properties);

  }


  @Override
  public void run() {
    Duration window = Duration.ofSeconds(10);
    kafkaConsumer.subscribe(Collections.singleton(IN_TOPIC));
    while (true) {

      long current = System.currentTimeMillis();
      Map<Long, List<EVSpan>> aggregate = new HashMap<>();

      while (Duration.ofMillis(System.currentTimeMillis() - current).minus(window).isNegative()) {
        ConsumerRecords<Long, EVSpan> records = kafkaConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Long, EVSpan> rec : records) {
          List<EVSpan> list = aggregate.get(rec.key());
          if (list == null) {
            list = new ArrayList<>(1);
            list.add(rec.value());
            aggregate.put(rec.key(), list);
          } else {
            list.add(rec.value());
          }
        }
      }

      produce(aggregate);

    }

  }

  private void produce(Map<Long, List<EVSpan>> aggregate) {

    for (Map.Entry<Long, List<EVSpan>> entry : aggregate.entrySet()) {
      ProducerRecord<Long, List<EVSpan>> rec =
          new ProducerRecord<>(OUT_TOPIC, entry.getKey(), entry.getValue());
      kafkaProducer.send(rec);

    }

  }

}

