package traceImporter.serdes;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class LinkedListSerializer<T> implements Serializer<LinkedList<T>> {

    private Serializer<T> inner;

    public LinkedListSerializer(Serializer<T> inner) {
        this.inner = inner;
    }

    // Default constructor needed by Kafka
    public LinkedListSerializer() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, LinkedList<T> queue) {
        final int size = queue.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        final Iterator<T> iterator = queue.iterator();
        try {
            dos.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = inner.serialize(topic, iterator.next());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize ArrayList", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {
        inner.close();
    }
}
