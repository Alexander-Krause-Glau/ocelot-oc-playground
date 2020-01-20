package traceImporter;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ListSerializer<T> implements Serializer<List<T>> {

    private Serializer<T> inner;

    public ListSerializer(Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public byte[] serialize(String topic, List<T> data) {
        final int size = data.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);

        final Iterator<T> it = data.iterator();
        try {
            dos.writeInt(size);
            while (it.hasNext()) {
                final byte[] bytes = inner.serialize(topic, it.next());
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }
}
