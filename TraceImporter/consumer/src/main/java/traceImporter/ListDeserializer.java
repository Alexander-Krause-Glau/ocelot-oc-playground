package traceImporter;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ListDeserializer<T> implements Deserializer<List<T>> {

    private Deserializer<T> inner;

    public ListDeserializer(Deserializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public List<T> deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        final LinkedList<T> list = new LinkedList<>();
        final DataInputStream dataInputStream =
                new DataInputStream(new ByteArrayInputStream(data));

        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                dataInputStream.read(valueBytes);
                list.add(inner.deserialize(topic, valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize ArrayList", e);
        }

        return list;
    }
}
