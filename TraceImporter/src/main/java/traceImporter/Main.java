package traceImporter;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.opencensus.proto.agent.trace.v1.*;

import java.io.IOException;

public class Main extends TraceServiceGrpc.TraceServiceImplBase {

  public static void main(String[] args) throws IOException, InterruptedException {
    Main m = new Main();


  }


  public Main() throws IOException, InterruptedException {
    super();

    Server s;

    s = ServerBuilder.forPort(12334).addService(this).build().start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        this.stop();
        System.err.println("*** server shut down");
      }
    });

    System.out.println("Started");
    s.awaitTermination();



  }

  @Override
  public StreamObserver<CurrentLibraryConfig> config(StreamObserver<UpdatedLibraryConfig> responseObserver) {
    return super.config(responseObserver);
  }

  @Override
  public StreamObserver<ExportTraceServiceRequest> export(StreamObserver<ExportTraceServiceResponse> responseObserver) {

    return super.export(responseObserver);
  }






}
