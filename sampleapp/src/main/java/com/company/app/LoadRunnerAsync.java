package com.company.app;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.common.util.concurrent.*;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LoadRunnerAsync<ResponseType> {
    private ListeningExecutorService executor;
    private Histogram histogram;
    private long requestsSubmitted = 0;
    private long testRunTimeMillis;
    private int threadCount;

    // I suggest not sending rps > 10e7, because that's the best a single load generator thread can do
    // other threads are used for listening for the responses of submitted requests
    public void runLoad(ApiFuture<ResponseType> request, long requestsPerSec, long runtimeSec, int threads)
            throws InterruptedException, ExecutionException {
        init(threads);

        System.out.println("Warming up ...");
        warmup(request);
        Thread.sleep(5000);
        System.out.println("Processing load ...");
        // starting original load test
        long loadStart = System.currentTimeMillis();
        while (runtimeSec-- > 0) {
            long start = System.currentTimeMillis();
            // send a batch of request
            submitRequests(request, requestsPerSec);
            long diff = System.currentTimeMillis() - start;
            // wait for remaining part of 1 sec (if there is any)
            if (diff < 999) Thread.sleep(1000 - diff);
        }
        this.testRunTimeMillis = (System.currentTimeMillis() - loadStart);
        System.out.println("Processing complete.");

        close();
    }

    private void init(int threads) {
        this.threadCount = threads;
        // let's hope to achieve max usage of processors and threading
        this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));

        // maximum trackable latency is 1 minute
        this.histogram = new AtomicHistogram(TimeUnit.MINUTES.toNanos(1), 3);

        this.requestsSubmitted = 0;
    }

    private void warmup(ApiFuture<ResponseType> request) throws ExecutionException, InterruptedException {
        // warmup 5 blocking request calls
        for(int i=0; i<5; i++) {
            request.get();
        }
        // reset counters
        requestsSubmitted = 0;
        histogram.reset();
    }

    // this just sends a batch of numRequests requests one after the other without any delay
    private void submitRequests(ApiFuture<ResponseType> request, long numRequests) {
        while(numRequests-- > 0) {
            long start = System.nanoTime();
            ApiFutures.addCallback(
                request,
                new ApiFutureCallback<ResponseType>() {
                    @Override
                    public void onSuccess(ResponseType res) {
                        histogram.recordValue(System.nanoTime() - start);
                    }

                    @Override
                    public void onFailure(Throwable th) {
                    }
                },
                executor
            );
            requestsSubmitted++;
        }
    }

    private void close() {
        executor.shutdownNow();
        System.out.println("Threads used: " + threadCount);
        System.out.println("Test runtime: " + testRunTimeMillis/1000 + " seconds");
        System.out.println("Total requests submitted: " + requestsSubmitted);
        System.out.println("Requests for which latencies recorded: " + histogram.getTotalCount());
    }

    public void saveHistogramToFile(String filePath) throws IOException {
        File outputFile = new File(filePath);
        try (FileOutputStream fout = new FileOutputStream(outputFile)) {
            histogram.outputPercentileDistribution(new PrintStream(fout), 1000.0);
        }
        System.out.println("Saved histogram to file: " + filePath);
    }
}
