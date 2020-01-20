package traceImporter.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.LinkedList;
import java.util.Map;

public class LinkedListSerde<T> implements Serde<LinkedList<T>> {

    private final Serde<LinkedList<T>> inner;

    public LinkedListSerde(Serde<T> serde) {
        inner =
            Serdes.serdeFrom(
                new LinkedListSerializer<>(serde.serializer()),
                new LinkedListDeserializer<>(serde.deserializer()));
    }

    @Override
    public Serializer<LinkedList<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<LinkedList<T>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
