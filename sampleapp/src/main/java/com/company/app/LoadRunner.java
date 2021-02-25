package com.company.app;

import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadRunner {
    private ExecutorService executor;
    private Histogram histogram;
    private long requestsSubmitted = 0;
    private AtomicInteger requestsExecutionStarted;
    private long testRunTimeMillis;

    // I suggest not sending rps > 10e7, because that's the best a single load generator thread can do
    // other threads are used for processing the submitted commands
    public void runLoad(Runnable request, long requestsPerSec, long runtimeSec) throws InterruptedException {
        init();

        warmup(request);

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

        close();
    }

    private void init() {
        // let's hope to achieve max usage of processors and threading
        this.executor = Executors.newWorkStealingPool();

        // maximum trackable latency is 1 minute
        this.histogram = new AtomicHistogram(TimeUnit.MINUTES.toNanos(1), 3);

        this.requestsExecutionStarted = new AtomicInteger(0);
    }

    private void warmup(Runnable request) {
        // warmup 10 request calls
        submitRequests(request, 10);
        // reset counters
        requestsExecutionStarted = new AtomicInteger(0);
        requestsSubmitted = 0;
    }

    // this just sends a batch of numRequests requests one after the other without any delay
    private void submitRequests(Runnable request, long numRequests) {
        while(numRequests-- > 0) {
            executor.submit(() -> {
                requestsExecutionStarted.incrementAndGet();
                long start = System.nanoTime();
                request.run();
                histogram.recordValue(System.nanoTime() - start);
            });
            requestsSubmitted++;
        }
    }

    private void close() {
        executor.shutdownNow();
        System.out.println("Test runtime: " + testRunTimeMillis/1000 + " seconds");
        System.out.println("Total requests submitted: " + requestsSubmitted);
        System.out.println("Requests for which execution started: " + requestsExecutionStarted.toString());
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
