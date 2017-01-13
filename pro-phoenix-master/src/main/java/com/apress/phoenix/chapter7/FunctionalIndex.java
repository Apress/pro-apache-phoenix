package com.apress.phoenix.chapter7;

import com.apress.phoenix.common.Application;
import com.apress.phoenix.common.Queries;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * functional indexes.
 */
@Slf4j
public class FunctionalIndex extends Application {

    public FunctionalIndex(Configuration configuration)  {
        super(configuration);
    }

    public static void main(String args[])
    {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        new FunctionalIndex(configuration).doIt();
    }

    public void doIt() {
        try {
            start();
            final String createQuery = Queries.sql("chapter7/supplier-create.sql");
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            // create functional index on mutable table.
            final String functionalIndexQuery = "CREATE INDEX SUPPLIER_UPPER_NAME_INDX ON " +
                    " SUPPLIER (UPPER(SUPPLIER_NAME)) INCLUDE (STATE)";

            statement.executeUpdate(functionalIndexQuery);

            final String stateQuery = "EXPLAIN SELECT UPPER(SUPPLIER_NAME), STATE FROM SUPPLIER";
            final ResultSet resultSet = statement.executeQuery(stateQuery);
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
