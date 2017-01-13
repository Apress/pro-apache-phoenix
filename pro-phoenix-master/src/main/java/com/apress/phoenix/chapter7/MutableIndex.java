package com.apress.phoenix.chapter7;

import com.apress.phoenix.common.Application;
import com.apress.phoenix.common.Queries;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.schema.PIndexState;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * mutable index creation and upserts.
 */
@Slf4j
public class MutableIndex extends Application {

    public MutableIndex(Configuration configuration)  {
        super(configuration);
    }

    public static void main(String args[])
    {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        // should phoenix do the rebuilding automatically or should it be a manual process of
        // explicitly giving the command "ALTER INDEX IF EXISTS <index_name> ON <data_table> REBUILD"
        configuration.set("phoenix.index.failure.handling.rebuild", "true");

        // by setting true, writes to data table will fail until the index is caught up with data table.
        configuration.set("phoenix.index.failure.block.write", "true");
        new MutableIndex(configuration).doIt();
    }

    public void doIt() {
        try {
            start();
            final String createQuery = Queries.sql("chapter7/customer-create.sql");
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            // upsert rows
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(100,'John','Gray', '1975-01-01','Lynden','Washington')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(101,'Donald','Duck', '1980-02-05','Nogales','Arizona')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(102,'Cindy','Hall', '1990-04-01','Plano','Texas')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(103,'Andrew','Hall', '1975-03-05','Plano','Texas')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(105,'Lisa','John', '1997-02-06','Kailua','Hawaii')");

            this.connection.commit();

            // create index on mutable table.
            final String mutableIndexQuery = "CREATE INDEX customer_state_indx ON customer (state)";
            statement = this.connection.createStatement();
            statement.executeUpdate(mutableIndexQuery);

            // check if index is active.
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            final String indexName = "CUSTOMER_STATE_INDX";
            ResultSet resultSet = databaseMetaData.getTables(null, null, indexName, null);
            PIndexState indexState = null;
            while(resultSet.next()) {
                final String state = resultSet.getString("INDEX_STATE");
                indexState = PIndexState.valueOf(state);
            }

            System.out.println(String.format(" The index [%s] is currently [%s] ", indexName, indexState != null
                    ? indexState.name() : "unavailable"));

            final String stateQuery = "EXPLAIN SELECT STATE FROM CUSTOMER";
            resultSet = statement.executeQuery(stateQuery);
            while(resultSet.next()) {
                String explainPlan = resultSet.getString(1);
                System.out.println(explainPlan);
            }
        } catch(Exception ex) {
            final String errorMsg = String.format("Error [%s] occurred while performing operations ", ex.getMessage());
            log.error(errorMsg, ex);
        } finally {
            close();
        }
    }
}
