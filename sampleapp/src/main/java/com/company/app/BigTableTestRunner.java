package com.company.app;
import com.google.api.core.ApiFutures;
import com.google.bigtable.v2.Row;
import org.HdrHistogram.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BigTableTestRunner {
    LoadRunnerAsync loadRunner;
    BigTableSimpleTestApi bigTableSimpleTestApi;

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        String testType = args[0];
        if (!testType.equals("read") && !testType.equals("write")) {
            throw new RuntimeException("Pass test type as either read or write");
        }

        long testRunTimeSec = Long.parseLong(args[1]);
        long rps = Long.parseLong(args[2]);
        int threads = Integer.parseInt(args[3]);
        String outputFilePath = args[4];
        System.out.printf(
            "Args supplied:\nTestType: %s\nTest Runtime in sec: %s\nRequests to send per sec: %s\nThreads: %s\nOutput file: %s\n",
            args[0], args[1], args[2], args[3], args[4]
        );
        BigTableTestRunner self = new BigTableTestRunner();
        self.init();
        self.runTest(testType, rps, testRunTimeSec, threads);
        self.cleanUp(outputFilePath);
    }

    void init() throws IOException {
        bigTableSimpleTestApi = new BigTableSimpleTestApi();
        bigTableSimpleTestApi.setup();
    }

    void runTest(String testType, long requestsPerSec, long runTimeSec, int threads) throws InterruptedException, ExecutionException {
        if (testType.equals("read")) {
            loadRunner = new LoadRunnerAsync<Row>();
            loadRunner.runLoad(bigTableSimpleTestApi.readAsync(), requestsPerSec, runTimeSec, threads);
        } else if (testType.equals("write")) {
            loadRunner = new LoadRunnerAsync<Void>();
            loadRunner.runLoad(bigTableSimpleTestApi.writeAsync(), requestsPerSec, runTimeSec, threads);
        } else {
            throw new RuntimeException("wrong test type provided: " + testType);
        }
    }

    void cleanUp(String outputFilePath) {
        bigTableSimpleTestApi.close();
        try {
            loadRunner.saveHistogramToFile(outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

