package traceImporter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class EVSpanTimestampKafkaExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
    int timestamp = -1;
    final EVSpan span = (EVSpan) record.value();

    if (span != null) {
      timestamp = span.getTimestamp();
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
