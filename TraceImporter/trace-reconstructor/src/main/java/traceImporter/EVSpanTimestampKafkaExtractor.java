package traceImporter;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EVSpanTimestampKafkaExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
    long timestamp = -1;
    final EVSpan span = (EVSpan) record.value();

    if (span != null) {
      //timestamp = Duration.ofNanos(span.getStartTime()).toMillis();
      //timestamp = Instant.ofEpochMilli(span.getStartTime()).toEpochMilli();
      return span.getStartTime();
    }
    if (timestamp < 0) {
      // Invalid timestamp! Attempt to estimate a new timestamp,
      // otherwise fall back to wall-clock time (processing-time).
      if (previousTimestamp >= 0) {
        return previousTimestamp;
      } else {
        return System.currentTimeMillis();
      }
    }


    return timestamp;
  }


}
