package traceImporter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;

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
    final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(data));

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
