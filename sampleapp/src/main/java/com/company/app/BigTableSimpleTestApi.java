package com.company.app;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Simple BigTable testing api. It writes / reads a single row, to a single table with a single column
 * family having a single column.
 */

class BigTableSimpleTestApi {
    private static final String PROJECT_ID = "data-platform-indodana-staging";
    private static final String INSTANCE_ID = "feature-store-stg";
    private final BigtableDataClient dataClient;
    private final BigtableTableAdminClient adminClient;

    private static final String TABLE_ID = "test_table";
    private static final String ROW_KEY = UUID.randomUUID().toString();
    private static final String COLUMN_FAMILY = "test_column_family";
    private static final String COLUMN_NAME = "test_column";
    private static final String COLUMN_VALUE = RandomStringUtils.randomAlphanumeric(1024);

    public BigTableSimpleTestApi() throws IOException {
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

    public void setup() {
        createTestTable();
        System.out.println("Writing sample row to table");
        writeToTestTable();
    }

    public void write() {
        writeToTestTable();
    }

    public void read() {
        readFromTestTable();
    }

    public void close() {
        deleteTestTable();
        dataClient.close();
        adminClient.close();
    }

    private void createTestTable() {
        if (!adminClient.exists(TABLE_ID)) {
            System.out.println("Creating table: " + TABLE_ID);
            CreateTableRequest createTableRequest = CreateTableRequest
                    .of(TABLE_ID)
                    .addFamily(COLUMN_FAMILY);
            adminClient.createTable(createTableRequest);
            System.out.printf("Table %s created successfully\n", TABLE_ID);
        } else {
            System.out.printf("Table %s already exists!\n", TABLE_ID);
        }
    }

    private void writeToTestTable() {
        try {
            RowMutation rowMutation = RowMutation
                    .create(TABLE_ID, ROW_KEY)
                    .setCell(COLUMN_FAMILY, COLUMN_NAME, COLUMN_VALUE);
            dataClient.mutateRow(rowMutation);
        } catch (NotFoundException e) {
            System.err.println("Failed to write to non-existent table: " + e.getMessage());
        }
    }

    private void readFromTestTable() {
        try {
            List<RowCell> row = dataClient
                    .readRow(TABLE_ID, ROW_KEY)
                    .getCells();
        } catch (NotFoundException e) {
            System.err.println("Failed to read from a non-existent table: " + e.getMessage());
        }
    }

    private void deleteTestTable() {
        System.out.println("Deleting table: " + TABLE_ID);
        try {
            adminClient.deleteTable(TABLE_ID);
            System.out.printf("Table %s deleted successfully%n", TABLE_ID);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }
}
