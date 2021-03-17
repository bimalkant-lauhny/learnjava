package com.company.app;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PostgresFeaturestoreLoadSimulator {
    String url = "jdbc:postgresql://127.0.0.1:5432/featurestores";
    String user = "bklauhny";
    String password = "admin";
    Connection dbConnection;
    Histogram histogram = new AtomicHistogram(TimeUnit.MINUTES.toNanos(5), 3);
    List<String> featureIds = new ArrayList<>();
    List<String[]> sample;
    List<Thread> threads = new ArrayList<>();
    static Long waitAfterPushSecs;
    static int numThreads;
    static String hdrOutputFile;
    AtomicInteger requestsCompleted = new AtomicInteger(0);
    static long simStartTime;

    public static void main(String[] args) throws IOException, CsvException, SQLException {
        numThreads = Integer.parseInt(args[0]);
        waitAfterPushSecs = Long.parseLong(args[1]);
        hdrOutputFile = args[2];
        System.out.printf(
                "Args supplied:\nThreads:%s\nWait after pushing(seconds): %s\nOutput file: %s\n",
                numThreads, waitAfterPushSecs, hdrOutputFile
        );

        PostgresFeaturestoreLoadSimulator sim = new PostgresFeaturestoreLoadSimulator();
        sim.setup();
        simStartTime = System.currentTimeMillis();
        try {
            sim.go();
            System.out.println("Sleeping for "+ waitAfterPushSecs +" seconds");
            Thread.sleep(waitAfterPushSecs*1000);
        } catch (Exception e) {
            System.out.println("Got error while running sim: " + e.getMessage());
        } finally {
            System.out.println("Simulation run time: " + (System.currentTimeMillis() - simStartTime)/1000 + " seconds");
            sim.close();
        }
    }

    public void setup() throws IOException, CsvException, SQLException {
        dbConnection = DriverManager.getConnection(url, user, password);
        CSVReader reader = new CSVReaderBuilder(new FileReader("src/main/java/com/company/app/features.csv"))
                .withSkipLines(1).build();
        for (String[] f: reader.readAll()) featureIds.add(f[0]);

        reader = new CSVReaderBuilder(new FileReader("src/main/java/com/company/app/sample.csv"))
                .withSkipLines(1).build();
        sample = reader.readAll();
    }

    public void close() {
        for (Thread t: threads) t.stop();
        System.out.println("" + requestsCompleted.get() + " requests done in " + (System.currentTimeMillis() - simStartTime)/1000 + " seconds");
        File outputFile = new File(hdrOutputFile);
        try (FileOutputStream fout = new FileOutputStream(outputFile)) {
            histogram.outputPercentileDistribution(new PrintStream(fout), 1000.0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void go() {
        for(int i=0; i<numThreads; i++) {
            int finalI = i;
            Thread t = new Thread(() -> {
                try {
                    extracted(finalI);
                } catch (Exception throwable) {
                    System.out.println("Got error while running thread: " + throwable.getMessage());
                }
            });
            threads.add(t);
            t.start();
        }

    }

    private void extracted(int finalI) throws SQLException {
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            PreparedStatement psDeleteOfs = con.prepareStatement("delete from object_features_scores where object_id = ?");
            PreparedStatement psDeleteOv = con.prepareStatement("delete from object_versions where id = ?::uuid");



            PreparedStatement psInsertOv = con.prepareStatement("INSERT INTO object_versions\n" +
                    "(id, object_id, apollo_timestamp_str, apollo_timestamp, storage_location, object_version_type, updated_at, created_at, tags, object_version_status)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?::objectversiontypeenum, ?, ?, ?, ?)\n");

            PreparedStatement psInsertOfs = con.prepareStatement("INSERT INTO object_features_scores\n" +
                    "(id, object_id, feature_id, feature_slug_name, scoring_date, value, default_score, split_value, description, result_type, created_at, updated_at, object_version_id)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);\n");
            List<UUID> featureIds = getFeatureIds(con);
            for(int i = sample.size()*finalI/numThreads; i<Math.min(sample.size(), sample.size()*(finalI+1)/numThreads); i++) {
                String[] rows = sample.get(i);
                String objectId = rows[0].trim();
                UUID objectVersionId = UUID.fromString(rows[1]);

                long start = System.nanoTime();

                psDeleteOfs.setObject(1, objectId);
                psDeleteOfs.executeUpdate();

                psDeleteOv.setObject(1, objectVersionId);
                psDeleteOv.executeUpdate();

                setObjectVersions(psInsertOv, objectId, objectVersionId);
                psInsertOv.executeUpdate();

                for (UUID featureId : featureIds) {
                    UUID ofsId = UUID.randomUUID();
                    setObjectFeatures(psInsertOfs, ofsId, objectId, featureId, objectVersionId);
                    psInsertOfs.addBatch();
                }
                psInsertOfs.executeBatch();
                psInsertOfs.clearBatch();

                histogram.recordValue(System.nanoTime() - start);
                long reqNumber = requestsCompleted.incrementAndGet();
                if (reqNumber%1000 == 0) {
                    System.out.println("" + reqNumber + " requests done in " + (System.currentTimeMillis() - simStartTime)/1000 + " seconds");
                }
            }

        }
    }

    private List<UUID> getFeatureIds(Connection con) throws SQLException {
        PreparedStatement ps = con.prepareStatement("select distinct id from features f ");
        ResultSet rs = ps.executeQuery();
        List<UUID> ret = new ArrayList<>();
        while(rs.next()) {
            ret.add(UUID.fromString(rs.getString(1)));
        }
        return ret;
    }

    public void executeUpdate(PreparedStatement ps, int expected) throws SQLException {
        int cnt = ps.executeUpdate();
        if(cnt != expected) {
//            throw new RuntimeException("expectaion mismatch");
            System.out.printf("expected %d got %d\n", expected, cnt);
        }
    }

    public void setObjectVersions(PreparedStatement ps, String object_id, UUID object_version) throws SQLException {
        int count = 1;
        // id
        ps.setObject(count++, object_version);
        // object_id
        ps.setObject(count++, object_id);
        // aplo_timestamp_str
        ps.setObject(count++, "1584072474291");
        // apollo_timestamp
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        // storage location
        ps.setObject(count++, "apollo/v1/archive/json/d99/orderId=CLI-8YRXMW71/d99-1584072467149.json");
        // object_version_type
        ps.setObject(count++, "BEFORE_APPROVAL");
        // updatedat
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        //createdat
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        //tags
        ps.setObject(count++, null);
        // object_version_status
        ps.setObject(count, "Pending");

    }


    public void setObjectFeatures(PreparedStatement ps, UUID id, String object_id, UUID feature_id, UUID object_version) throws SQLException {
        int count = 1;
        // id
        ps.setObject(count++, id);
        // object_id
        ps.setObject(count++, object_id);
        // feature_id
        ps.setObject(count++, feature_id);
        // feature_slug_name
        ps.setObject(count++, "days_from_oldest_credit_card_payment_date");
        // scoring date
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        // value
        ps.setObject(count++, "1.3333333333333333");
        // default score
        ps.setObject(count++, 0);
        // split value
        ps.setObject(count++, null);
        // description
        ps.setObject(count++, null);
        // result_type
        ps.setObject(count++, "NUMERIC_LIMIT");
        // created_at
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        // updatedat
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        // object_version
        ps.setObject(count, object_version);
    }

    String ss = "{\"sharath1\":\""+ StringUtils.repeat('a', 3000)+"\"}";
    String tt = "{\"sharath2\":\""+StringUtils.repeat('a', 300)+"\"}";
}
