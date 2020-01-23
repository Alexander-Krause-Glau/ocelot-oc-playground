package traceImporter;

import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    new SpanBatchProducerStream().run();
    //new SpanSingleProducerStream().run();
  }
}
