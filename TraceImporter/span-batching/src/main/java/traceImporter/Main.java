package traceImporter;

import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    // new Thread(new SpanBatchProducer()).start();
    // new Thread(new SpanSingleProducer()).start();
    new SpanSingleProducerStream().run();
  }
}
