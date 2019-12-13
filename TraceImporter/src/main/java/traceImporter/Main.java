package traceImporter;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.opencensus.proto.agent.trace.v1.*;

import java.io.IOException;

public class Main {

  public static void main(String[] args) throws IOException, InterruptedException {
    new Thread(new KafkaTraceConsumer()).start();
  }







}
