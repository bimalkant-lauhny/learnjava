package com.company.app;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.common.util.concurrent.MoreExecutors;
import org.HdrHistogram.*;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A simple example of using HdrHistogram: run for 20 seconds collecting the
 * time it takes to perform a simple Datagram Socket create/close operation,
 * and report a histogram of the times at the end.
 */

public class BTTest {
    // A Histogram covering the range from 1 nsec to 1 hour with 3 decimal point resolution:
    ExecutorService executor;
    Histogram histogram = new Histogram(3600000000000L, 3);

    BigTableSimpleTestApi bigTableSimpleTestApi;

    static long WARMUP_TIME_MSEC = 5000;

    void init() throws IOException {
        bigTableSimpleTestApi = new BigTableSimpleTestApi();
        bigTableSimpleTestApi.setup();
        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(16));
    }

    void recordTimeForTest() {
        long start = System.nanoTime();
        ApiFuture<Void> responseFuture = bigTableSimpleTestApi.writeAsync();
        ApiFutures.addCallback(
            responseFuture,
            new ApiFutureCallback<Void>() {
                @Override
                public void onSuccess(Void res) {
                    histogram.recordValue(System.nanoTime() - start);
                }

                @Override
                public void onFailure(Throwable th) {
                }
            },
            executor
        );
    }

    public static void main(final String[] args) throws IOException {
        long testRunTimeSec = Long.parseLong(args[0]);
        String outputFilePath = args[1];
        System.out.printf(
                "Args supplied:\nTest Runtime in sec: %s\nOutput file: %s\n",
                args[0], args[1]
        );
        BTTest self = new BTTest();
        self.init();
        self.runTest(testRunTimeSec*1000);
        self.cleanUp(outputFilePath);

    }

    void runTest(long runTimeMillis) {
        long startTime = System.currentTimeMillis();
        long now;

        do {
            recordTimeForTest();
            now = System.currentTimeMillis();
        } while (now - startTime < runTimeMillis);
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