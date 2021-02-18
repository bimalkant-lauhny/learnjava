package com.company.app;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.*;

/**
 * Hello world!
 */
public class BigTableTesting {
    private static final String PROJECT_ID = "data-platform-indodana-staging";
    private static final String INSTANCE_ID = "feature-store-stg";
    private static final String TABLE_ID = "featurestore";
    private static final String ROW_KEY = "ATH-3K34BD2L";
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;
    private long testRuns;

    public static void main(String[] args) throws Exception {
        long testRuns = 100;
        if (args.length >= 2) {
            testRuns = Long.parseLong(args[1]);
        }
        System.out.println(testRuns);
        BigTableTesting helloWorld = new BigTableTesting(testRuns);
        helloWorld.run();
    }

    public BigTableTesting(long testRuns) throws IOException {
        this.testRuns = testRuns;
        BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();

        dataClient = BigtableDataClient.create(settings);

        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build();

        adminClient = BigtableTableAdminClient.create(adminSettings);
    }

    public void run() throws Exception {
        createTestTable();
        runWriteTest();
        runReadTest();
        deleteTestTable();
        close();
    }

    private void runWriteTest() {
        long startTime = System.currentTimeMillis();
        for (long i=0; i<testRuns; ++i) {
            writeToTestTable();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Avg Write Runtime: " + (endTime - startTime)/testRuns + " ms");
    }

    private void runReadTest() {
        long startTime = System.currentTimeMillis();
        for (long i=0; i<testRuns; ++i) {
            readFromTestTable();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Avg Read Runtime: " + (endTime - startTime)/testRuns + " ms");
    }

    private void createTestTable() {
        createTable(TABLE_ID, Arrays.asList(
                "rules", "features", "object_feature_score_variables", "object_features_scores", "object_versions",
                "scoring_models", "object_scoring_results"
        ));
    }

    private void writeToTestTable() {
        writeToTable(TABLE_ID, ROW_KEY);
    }

    private void readFromTestTable() {
        readSingleRow(TABLE_ID, ROW_KEY);
    }

    private void deleteTestTable() {
        deleteTable(TABLE_ID);
    }

    public void createTable(String tableId, List<String> colFamilies) {
        if (!adminClient.exists(tableId)) {
            System.out.println("Creating table: " + tableId);
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
            for (String family: colFamilies) {
                createTableRequest = createTableRequest.addFamily(family);
            }
            adminClient.createTable(createTableRequest);
            System.out.printf("Table %s created successfully\n", tableId);
        } else {
            System.out.printf("Table %s already exists!\n", tableId);
        }
    }

    public void writeToTable(String tableId, String rowKey) {
        try {
            RowMutation rowMutation = RowMutation.create(tableId, rowKey)
                    .setCell("object_feature_score_variables", "variables", "{\"applicantMobilePhoneNumber\":\"6285921062102\",\"appicantHomePhoneNumber\":\"6285921062102\",\"contactMobilePhone\":[\"6285710867487\"],\"contactFixedLine\":[null]}")
                    .setCell("object_feature_score_variables", "created_at", "2021-02-01T03:53:26.577Z")
                    .setCell("object_feature_score_variables", "updated_at", "2021-02-01T03:53:52.561Z")
                    .setCell("object_features_scores", "scoring_date", "2021-02-01T03,53,52.560Z")
                    .setCell("object_features_scores", "value", "0")
                    .setCell("object_features_scores", "default_score", "0")
                    .setCell("object_features_scores", "split_value", "null")
                    .setCell("object_features_scores", "description", "null")
                    .setCell("object_features_scores", "result_type", "NUMERIC_LIMIT")
                    .setCell("object_features_scores", "created_at", "2021-02-01T03,53,28.180Z")
                    .setCell("object_features_scores", "updated_at", "2021-02-01T03,53,52.560Z")
                    .setCell("object_versions", "apollo_timestamp_str", "1612151629633")
                    .setCell("object_versions", "apollo_timestamp", "2021-02-01T03,53,49.000Z")
                    .setCell("object_versions", "storage_location", "apollo/v1/archive/json/d99/orderId=ATH-3K34BD2L/d99-1612151629633.json")
                    .setCell("object_versions", "object_version_type", "BEFORE_APPROVAL")
                    .setCell("object_versions", "updated_at", "2021-02-01T03,53,51.794Z")
                    .setCell("object_versions", "created_at", "2021-02-01T03,53,51.794Z")
                    .setCell("object_versions", "tags", "null")
                    .setCell("object_versions", "object_version_status", "Pending")
                    .setCell("features", "feature_name", "pefindo individuals phone number match")
                    .setCell("features", "feature_slug_name", "pefindo_individuals_phone_number_match")
                    .setCell("features", "is_active", "true")
                    .setCell("features", "created_at", "2020-08-19T04,01,44.403Z")
                    .setCell("features", "updated_at", "2020-08-19T04,01,44.403Z")
                    .setCell("features", "description", "Returns 1 if applicant's submitted mobile or home phone number matches with pefindo individuals phone number or fixed line information, 0 if doesn't match, and -99 if pefindo individuals phone number is empty.")
                    .setCell("features", "feature_type", "INTEGER")
                    .setCell("features", "is_production", "true")
                    .setCell("rules", "rule_name", "Pefindo Data Rule")
                    .setCell("rules", "rule_slug_name", "PefindoDataRule")
                    .setCell("rules", "rule_description", "\"Pefindo Data Rule\"")
                    .setCell("rules", "is_active", "true")
                    .setCell("rules", "created_at", "2020-07-28T08:15:50.649Z")
                    .setCell("rules", "updated_at", "2020-07-28T08:15:50.649Z")
                    .setCell("scoring_models", "model_name", "identity_score_scoring_criteria_indodana_20210126")
                    .setCell("scoring_models", "model_json", "{\"rules\": {\"IziDataRule\": {\"izi_mobile_phone_number_ages\": [{\"score\": 39, \"in\": [\"12month+\", \"10-12month\"]}, {\"score\": 2, \"in\": [\"8-10month\", \"0-1month\", \"1-2month\", \"6-8month\", \"3-4month\", \"2-3month\", \"5-6month\", \"4-5month\"]}, {\"score\": -19, \"in\": [\"-99\", \"-999\"]}, {\"score\": 0, \"in\": [\"UNBINNED\"], \"default\": true}]}, \"DtlabDataRule\": {\"verified_izi_mobile_phone_whatsapp\": [{\"score\": 6, \"in\": [\"not verified\"]}, {\"score\": -35, \"in\": [\"no\"]}, {\"score\": 63, \"in\": [\"verified\"]}, {\"score\": 0, \"in\": [\"UNBINNED\"], \"default\": true}], \"email_age_ever_name_match\": [{\"min\": \"-inf\", \"max\": -99, \"score\": -20}, {\"min\": -99, \"max\": -1, \"score\": -11}, {\"min\": 5, \"max\": \"inf\", \"score\": 42}], \"phone_age_ever_name_match\": [{\"min\": \"-inf\", \"max\": -99, \"score\": -11}, {\"min\": -99, \"max\": -1, \"score\": 0}, {\"min\": 5, \"max\": \"inf\", \"score\": 25}]}, \"PefindoDataRule\": {\"pefindo_max_phone_number_match\": [{\"min\": \"-inf\", \"max\": -99, \"score\": -11}, {\"min\": -99, \"max\": 0, \"score\": 0}, {\"min\": 0, \"max\": \"inf\", \"score\": 82}]}, \"HibpDataRule\": {\"hibp_name_match\": [{\"min\": -99, \"max\": 0, \"score\": -9}, {\"min\": 0, \"max\": \"inf\", \"score\": 31}]}}}")
                    .setCell("scoring_models", "segmentation", "")
                    .setCell("scoring_models", "is_default", "true")
                    .setCell("scoring_models", "default_score", "460.0")
                    .setCell("scoring_models", "minimum_threshold", "330.0")
                    .setCell("scoring_models", "maximum_threshold", "0.0")
                    .setCell("scoring_models", "is_active", "true")
                    .setCell("scoring_models", "created_at", "2021-01-26T04:34:40.347Z")
                    .setCell("scoring_models", "updated_at", "2021-01-26T04:34:40.347Z")
                    .setCell("scoring_models", "segment_indicators", "{\"income\": \"*\", \"model_type\": \"IDENTITY\", \"product_type\": \"*\", \"application_type\": \"*\"}")
                    .setCell("object_scoring_results", "scoring_date", "2021-02-01T03:53:52.958Z")
                    .setCell("object_scoring_results", "final_score", "402.0")
                    .setCell("object_scoring_results", "rules_output", "{\"count_rejected_or_not_qualified_prev_application_by_nik\": {\"count_rejected_or_not_qualified_prev_application_by_nik\": 1, \"score\": -7.0}, \"age_female\": {\"gender\": \"MALE\", \"age\": -99, \"score\": -2.0}, \"age_male\": {\"gender\": \"MALE\", \"age\": 37, \"score\": 2.0}, \"question_of_other_loan\": {\"question_other_loan\": \"TIDAK\", \"score\": -2.0}, \"gps_density\": {\"gps_density\": \"jabodetabek_high\", \"city_population\": 2622, \"group_id\": \"289,257\", \"score\": 4.0}, \"company_gps_match_in_seven_days\": {\"count_match_within_seven_days\": 0, \"score\": -6.0}, \"applicant_profession\": {\"profession\": \"WIRASWASTA\", \"score\": 0.0}, \"education\": {\"applicant_last_education_level\": \"SMA\", \"score\": -3.0}, \"number_of_substrings_in_reference_fullname\": {\"score\": 1.0}, \"length_of_employment\": {\"working_time_in_month\": 49, \"working_time_start\": \"2017-01-01 00:00:00\", \"score\": -2.0}, \"income\": {\"monthly_income\": 10000000.0, \"score\": 2.0}, \"number_of_high_risk_games\": {\"count\": 0, \"app_list\": [], \"score\": 2.0}, \"number_of_low_risk_lifestyle_apps\": {\"count\": 0, \"app_list\": [], \"score\": -4.0}, \"number_of_low_risk_loan_apps\": {\"count\": 0, \"app_list\": [], \"score\": -1.0}, \"number_of_non_ojk_loan_apps\": {\"number_of_non_ojk_loan_apps\": 0, \"score\": 3.0}, \"number_of_p2p_payment_received\": {\"number_of_p2p_payment_received\": 0, \"score\": -5.0}, \"number_of_clear_salary_match_label_1m\": {\"is_bank_verified\": true, \"score\": -1.0}, \"max_salary_only_label_3m\": {\"max_salary_only_label_3m_verified\": false, \"max_verified_bank_income\": \"\", \"score\": -2.0}, \"average_balance_90d\": {\"bank_id_keys\": \"5d10e5c2-0a4c-4859-8982-1643a575a49a\", \"score\": 5.0}, \"number_of_low_risk_wifi_ssid\": {\"number_of_low_risk_wifi_ssid\": -99, \"score\": 2.0}, \"izi_mobile_phone_number_ages\": {\"scrapper_status\": \"OK\", \"score\": 2.0}, \"izi_whatsapp_avaibility\": {\"scrapper_status\": \"OK\", \"score\": 5.0}, \"izi_reference_mobile_phone_number_whatsapp_availability\": {\"scrapper_status\": \"OK\", \"score\": 4.0}, \"izi_reference_mobile_phone_number_multi_inquiries_total\": {\"scrapper_status\": \"OK\", \"score\": 4.0}, \"izi_max_multi_inquiries_14d\": {\"izi_mobile_phone_number_multi_inquiries_14d\": 1, \"izi_id_multi_inquiries_14d\": 1, \"score\": 7.0}, \"izi_max_multi_inquiries_90d\": {\"izi_mobile_phone_number_multi_inquiries_90d\": 10, \"izi_id_multi_inquiries_90d\": 10, \"score\": -3.0}, \"izi_ratio_id_phone_number_14d\": {\"izi_mobile_phone_number_multi_inquiries\": 1, \"izi_id_multi_inquiries\": 1, \"score\": 2.0}, \"percentage_of_calculated_max_day_past_due_3_contract_due_date_before_submit\": {\"calculated_max_day_past_due_count\": 0, \"fdc_filter_loan_status_count\": 5, \"score\": 10.0}, \"percentage_of_calculated_max_day_past_due_90_contract_due_date_before_submit\": {\"calculated_max_day_past_due_count\": 0, \"fdc_filter_loan_status_count\": 5, \"score\": 5.0}, \"avg_amount_disbursed_total\": {\"score\": 4.0}, \"weighted_count_paylater\": {\"count_contract\": 13, \"score\": -2.0}, \"discounted_sum_outstanding_late_1y\": {\"score\": 10.0}, \"number_of_website_breach_occured_on_email\": {\"website_breached_names\": [\"Tokopedia\"], \"score\": 5.0}, \"phone_model_class\": {\"model\": \"iPhone7,2\", \"score\": 24.0}, \"phone_age_ever_name_match\": {\"match\": true, \"score\": 1.0}, \"email_age_ever_name_match\": {\"match\": true, \"score\": 1.0}}")
                    .setCell("object_scoring_results", "created_at", "2021-02-01T03:53:52.958Z")
                    .setCell("object_scoring_results", "updated_at", "2021-02-01T03:53:52.958Z");
            dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println("Failed to write to non-existent table: " + e.getMessage());
        }
    }

    public void readSingleRow(String tableId, String rowKey) {
        try {
            Row row = dataClient.readRow(tableId, rowKey);
            row.getCells();
        } catch (NotFoundException e) {
            System.err.println("Failed to read from a non-existent table: " + e.getMessage());
        }
    }

    public void deleteTable(String tableId) {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }

    public void close() {
        dataClient.close();
        adminClient.close();
    }

}
