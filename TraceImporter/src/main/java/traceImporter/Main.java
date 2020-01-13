package traceImporter;

import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    KafkaSpanProducer ksp = new KafkaSpanProducer();

    new Thread(new KafkaDumpSpanConsumer(ksp)).start();
    new Thread(new SpanBatchProducer()).start();
    new Thread(new SpanBatchConsumer()).start();
  }







}
