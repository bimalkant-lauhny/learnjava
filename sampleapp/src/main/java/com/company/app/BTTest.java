package com.company.app;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.common.util.concurrent.MoreExecutors;
import org.HdrHistogram.*;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple example of using HdrHistogram: run for 20 seconds collecting the
 * time it takes to perform a simple Datagram Socket create/close operation,
 * and report a histogram of the times at the end.
 */

public class BTTest {
    // A Histogram covering the range from 1 nsec to 1 hour with 3 decimal point resolution:
    ExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(16));;
    Histogram histogram = new AtomicHistogram(TimeUnit.MINUTES.toNanos(1), 3);

    BigTableSimpleTestApi bigTableSimpleTestApi;

    AtomicInteger requestsSubmitted = new AtomicInteger(0);
    long submitRps;
    long runtimeSec;

    public static void main(final String[] args) throws IOException {
        String testType = args[0];
        if (!testType.equals("read") && !testType.equals("write")) {
            throw new RuntimeException("Pass test type as either read or write");
        }

        long testRunTimeSec = Long.parseLong(args[1]);
        long rps = Long.parseLong(args[2]);
        String outputFilePath = args[3];
        System.out.printf(
                "Args supplied:\nTestType: %s\nTest Runtime in sec: %s\nRequests to send per sec: %s\nOutput file: %s\n",
                testType, testRunTimeSec, rps, outputFilePath
        );

        BTTest self = new BTTest();
        self.init(rps, testRunTimeSec);
        try {
            self.runTest(testType);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        self.cleanUp(outputFilePath);

    }

    void init(long submitRps, long runtimeSec) throws IOException {
        this.submitRps = submitRps;
        this.runtimeSec = runtimeSec;
        bigTableSimpleTestApi = new BigTableSimpleTestApi();
        bigTableSimpleTestApi.setup();
    }

    void runTest(String testType) throws InterruptedException {
        System.out.println("Processing load ...");
        // starting original load test
        long loadStart = System.currentTimeMillis();
        while (runtimeSec-- > 0) {
            long start = System.currentTimeMillis();
            // send a batch of request
            long numRequests = submitRps;
            while(numRequests-- > 0) {
                if (testType.equals("read")) submitReadRequest();
                else submitWriteRequest();
            }
            long diff = System.currentTimeMillis() - start;
            // wait for remaining part of 1 sec (if there is any)
            if (diff < 999) Thread.sleep(1000 - diff);
        }
        long testRunTimeMillis = (System.currentTimeMillis() - loadStart);
        System.out.println("Processing complete.");

        System.out.println("Test runtime: " + testRunTimeMillis/1000 + " seconds");
        System.out.println("Total requests submitted: " + requestsSubmitted);
        System.out.println("Requests for which latencies recorded: " + histogram.getTotalCount());
    }

    void submitReadRequest() {
        long start = System.nanoTime();
        ApiFutures.addCallback(
            bigTableSimpleTestApi.readAsync(),
            new ApiFutureCallback<Row>() {
                @Override
                public void onSuccess(Row res) { histogram.recordValue(System.nanoTime() - start); }

                @Override
                public void onFailure(Throwable th) { }
            },
            executor
        );
        requestsSubmitted.incrementAndGet();
    }

    void submitWriteRequest() {
        long start = System.nanoTime();
        ApiFutures.addCallback(
                bigTableSimpleTestApi.writeAsync(),
                new ApiFutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void res) { histogram.recordValue(System.nanoTime() - start); }

                    @Override
                    public void onFailure(Throwable th) { }
                },
                executor
        );
        requestsSubmitted.incrementAndGet();
    }

    void cleanUp(String outputFilePath) {
        executor.shutdownNow();
        bigTableSimpleTestApi.close();
        File outputFile = new File(outputFilePath);
        try (FileOutputStream fout = new FileOutputStream(outputFile)) {
            histogram.outputPercentileDistribution(new PrintStream(fout), 1000.0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Saved histogram to file: " + outputFilePath);
    }
}