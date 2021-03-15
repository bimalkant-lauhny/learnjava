/**
Author: Sharath Gururaj
**/

package sha;


import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import static sha.Utils.dumps;
import static sha.Utils.readJsonFromClasspath;

public class App {
    private static final Logger log = LogManager.getLogger();
    private static Settings s;
    private List<String[]> r;

    public static void main(String[] args) {
        try {
            App obj = getObj(args);
            obj.go();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * this method can be conveniently used in test cases to get the object under test
     */
    public static App getObj(String[] args) {
        App obj = new App();


        String path = "settings.json";

        try {
            if (args.length > 0) {
                path = args[0];
            }
            s = readJsonFromClasspath(path, Settings.class);
        } catch (Exception e) {
            s = new Settings();
        }
        // log.info("Using settings:{}", dumps(s));

        return obj;
    }

    public static class Settings {
        public String dummy = "";

        // required for jackson
        public Settings() {
        }
    }

    String url = "jdbc:postgresql://localhost:5432/featurestores";
    String user = "testuser";
    String password = "testpass";
    Utils.LatencyTimer aa = new Utils.LatencyTimer("aa");
    Utils.LatencyTimer bb = new Utils.LatencyTimer("bb");
    Utils.LatencyTimer cc = new Utils.LatencyTimer("cc");
    Utils.LatencyTimer dd = new Utils.LatencyTimer("dd");
    Utils.LatencyTimer ee = new Utils.LatencyTimer("ee");
    //            Utils.LatencyTimer ff = new Utils.LatencyTimer("ff");
    Utils.Timer t = new Utils.Timer("trpt");

    /**
     * All teh code from here:
     */
    private void go() throws Exception {

        r = null;
        try (CSVReader reader = new CSVReader(new FileReader("random_sample.csv"))) {
            r = reader.readAll();
        }


        int threads = 2;
        for(int i=0; i<threads; i++) {
            int finalI = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        extracted(finalI, threads);
                    } catch (SQLException throwables) {
                        log.error("", throwables);
                    }
                }
            });
            t.start();
        }


    }

    private void extracted(int finalI, int threads) throws SQLException {
        try (Connection con = DriverManager.getConnection(url, user, password)) {
            PreparedStatement psDeleteOfsv = con.prepareStatement("delete from object_feature_score_variables  ofsv\n" +
                    "using  object_features_scores ofs \n" +
                    "where ofs.id = ofsv.object_feature_score_id \n" +
                    "and ofs.object_id = ?\n" +
                    "\n");
            PreparedStatement psDeleteOfs = con.prepareStatement("delete from object_features_scores where object_id = ?");
            PreparedStatement psDeleteOv = con.prepareStatement("delete from object_versions where id = ?::uuid");



            PreparedStatement psInsertOv = con.prepareStatement("INSERT INTO object_versions\n" +
                    "(id, object_id, apollo_timestamp_str, apollo_timestamp, storage_location, object_version_type, updated_at, created_at, tags, object_version_status)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?::objectversiontypeenum, ?, ?, ?, ?)\n");

            PreparedStatement psInsertOfs = con.prepareStatement("INSERT INTO object_features_scores\n" +
                    "(id, object_id, feature_id, feature_slug_name, scoring_date, value, default_score, split_value, description, result_type, created_at, updated_at, object_version_id)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);\n");

            PreparedStatement psInsertOfsv = con.prepareStatement("INSERT INTO object_feature_score_variables\n" +
                    "(id, object_feature_score_id, variables, created_at, updated_at)\n" +
                    "VALUES(?, ?, ?::json, ?, ?);\n");


            PreparedStatement psInsertOsr = con.prepareStatement("INSERT INTO object_scoring_results\n" +
                    "(id, object_id, scoring_model_id, scoring_model_name, scoring_date, final_score, rules_output, created_at, updated_at)\n" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);\n");

            int cnt = 0;
            List<UUID> featureIds = getFeatureIds(con);
            for(int i=r.size()*finalI/threads; i<Math.min(r.size(), r.size()*(finalI+1)/threads); i++) {
                String[] rows = r.get(i);
//            for (String[] rows : r) {
                t.count();
                String objectId = rows[0].trim();
                UUID objectVersionId = UUID.fromString(rows[1]);

                long a = System.nanoTime();
                psDeleteOfsv.setObject(1, objectId);
                executeUpdate(psDeleteOfsv, -1);

                long b = System.nanoTime();
                aa.count(b-a);
                psDeleteOfs.setObject(1, objectId);
                executeUpdate(psDeleteOfs, -1);

                long c = System.nanoTime();
                bb.count(c-b);
                psDeleteOv.setObject(1, objectVersionId);
                executeUpdate(psDeleteOv, 1);

                long d = System.nanoTime();
                cc.count(d-c);
                setObjectVersions(psInsertOv, objectId, objectVersionId);
                executeUpdate(psInsertOv, 1);
                long e = System.nanoTime();
                dd.count(e-d);
                for (UUID featureId : featureIds) {
                    UUID ofsId = UUID.randomUUID();
                    setObjectFeatures(psInsertOfs, ofsId, objectId, featureId, objectVersionId);
                    psInsertOfs.addBatch();

//                    setObjectScoreVariables(psInsertOfsv, UUID.randomUUID(), ofsId);
//                    psInsertOfsv.addBatch();
                }
                psInsertOfs.executeBatch();
                psInsertOfs.clearBatch();

                long f = System.nanoTime();
                ee.count(f-e);
//                psInsertOfsv.executeBatch();
//                psInsertOfsv.clearBatch();
//                long g = System.nanoTime();
//                ff.count(g-f);
//                log.info("finished row {}", cnt++);

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

    public void extractFeatures() throws SQLException, IOException {

        try (Connection con = DriverManager.getConnection(url, user, password)) {
            PreparedStatement ps = con.prepareStatement("select * from object_features_scores where object_id=?");
            UUID uuid = UUID.fromString("ff5a67a8-6c20-11eb-bf22-37703c3c020d");
            List<Map<String, Object>> rows = new ArrayList<>();
            ps.setObject(1, uuid.toString());
            ResultSet rs = ps.executeQuery();
            int columnCount = rs.getMetaData().getColumnCount();

            while(rs.next()) {
                Map<String, Object> cols = new HashMap<>();

                for(int i=0; i<columnCount; i++) {
                    cols.put(rs.getMetaData().getColumnName(i+1), rs.getObject(i+1));
                }
                rows.add(cols);
            }
            Files.write(Paths.get("rows.json"), dumps(rows).getBytes(StandardCharsets.UTF_8));
            ps.close();
            rs.close();
        }
    }

    public void executeUpdate(PreparedStatement ps, int expected) throws SQLException {
        int cnt = ps.executeUpdate();
        if(cnt != expected && expected != -1) {
//            throw new RuntimeException("expectaion mismatch");
            log.warn("expectaion mismatch, expected {} got {}", expected, cnt);
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

    public void setObjectScoreVariables(PreparedStatement ps, UUID id, UUID ofsid) throws SQLException {
        int count = 1;
        // id
        ps.setObject(count++, id);
        // object_feature_score_id
        ps.setObject(count++, ofsid);
        // variables
        ps.setObject(count++, tt);
        // createdat
        ps.setObject(count++, new Date(new java.util.Date().getTime()));
        //updatedat
        ps.setObject(count, new Date(new java.util.Date().getTime()));
    }

    String ss = "{\"sharath1\":\""+StringUtils.repeat('a', 3000)+"\"}";
    String tt = "{\"sharath2\":\""+StringUtils.repeat('a', 300)+"\"}";

    public void setObjectScoringResult(PreparedStatement ps, String objectId) throws SQLException {
        int count = 1;

        // id
        ps.setObject(count++, UUID.randomUUID());

        // object_id
        ps.setObject(count++, objectId);

        // scoring_model_id
        ps.setObject(count++, UUID.fromString("b0eb910e-60da-423b-8616-d71cd853bea7"));

        // final_score
        ps.setObject(count++, 100);

        // rules_output
        ps.setObject(count++, ss);

        // createdat
        ps.setObject(count++, new Date(new java.util.Date().getTime()));

        // updatedat
        ps.setObject(count, new Date(new java.util.Date().getTime()));



    }
}
