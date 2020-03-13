package traceImporter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class EVSpanTimestampKafkaExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        final EVSpan span = (EVSpan) record.value();

        if (span != null) {
            // timestamp = Duration.ofNanos(span.getStartTime()).toMillis();
            // timestamp = Instant.ofEpochMilli(span.getStartTime()).toEpochMilli();
            return Instant
                .ofEpochSecond(span.getStartTime().getSeconds(),
                    span.getStartTime().getNanoAdjust())
                .toEpochMilli();
        }

        // Invalid timestamp! Attempt to estimate a new timestamp,
        // otherwise fall back to wall-clock time (processing-time).
        if (previousTimestamp >= 0) {
            return previousTimestamp;
        } else {
            return System.currentTimeMillis();
        }


    }


}
