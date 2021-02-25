package com.company.app;
import org.HdrHistogram.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class BigTableTestRunner {
    LoadRunner loadRunner;
    BigTableSimpleTestApi bigTableSimpleTestApi;

    public static void main(String[] args) throws InterruptedException, IOException {
        String testType = args[0];
        if (!testType.equals("read") && !testType.equals("write")) {
            throw new RuntimeException("Pass test type as either read or write");
        }

        Long testRunTimeSec = Long.parseLong(args[1]);
        Long rps = Long.parseLong(args[2]);
        String outputFilePath = args[3];

        BigTableTestRunner self = new BigTableTestRunner();
        self.init();
        self.runTest(testType, rps, testRunTimeSec);
        self.cleanUp(outputFilePath);
    }

    void init() throws IOException {
        bigTableSimpleTestApi = new BigTableSimpleTestApi();
        bigTableSimpleTestApi.setup();
        loadRunner = new LoadRunner();
    }

    void runTest(String testType, Long requestsPerSec, Long runTimeSec) throws InterruptedException {
        if (testType.equals("read")) {
            loadRunner.runLoad(() -> bigTableSimpleTestApi.read(), requestsPerSec, runTimeSec);
        } else if (testType.equals("write")) {
            loadRunner.runLoad(() -> bigTableSimpleTestApi.write(), requestsPerSec, runTimeSec);
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

