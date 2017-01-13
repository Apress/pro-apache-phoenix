package com.apress.phoenix.chapter7;

import com.apress.phoenix.common.Application;
import com.apress.phoenix.common.Queries;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.PIndexState;
import org.junit.Assert;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * immutable index
 */
@Slf4j
public class ImmutableIndex extends Application {

    public ImmutableIndex(Configuration configuration)  {
        super(configuration);
    }

    public static void main(String args[])
    {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        new ImmutableIndex(configuration).doIt();
    }

    public void doIt() {
        try {
            start();
            String createQuery = Queries.sql("chapter7/customer-create.sql");
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            createQuery = Queries.sql("chapter7/orders-create-immutable.sql");
            statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            // upsert rows
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(100,'John','Gray', '1975-01-01','Lynden','Washington')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(101,'Donald','Duck', '1980-02-05','Nogales','Arizona')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(102,'Cindy','Hall', '1990-04-01','Plano','Texas')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(103,'Andrew','Hall', '1975-03-05','Plano','Texas')");
            statement.executeUpdate("UPSERT INTO CUSTOMER VALUES(105,'Lisa','John', '1997-02-06','Kailua','Hawaii')");

            statement.executeUpdate("UPSERT INTO ORDERS VALUES(10248,101,'1998-4-7',10000, 10)");
            statement.executeUpdate("UPSERT INTO ORDERS VALUES(10249,101,'1998-5-7',20000, 20)");
            statement.executeUpdate("UPSERT INTO ORDERS VALUES(10340,104,'1998-10-10',10000, 5)");
            statement.executeUpdate("UPSERT INTO ORDERS VALUES(10301,101,'1998-6-7',30000, 10)");

            this.connection.commit();

            // explain query without index.
            String amountQuery = "EXPLAIN SELECT SUM(AMOUNT) FROM ORDERS GROUP BY CUST_ID";
            ResultSet resultSet = statement.executeQuery(amountQuery);
            while(resultSet.next()) {
                String explainPlan = resultSet.getString(1);
                System.out.println(explainPlan);
            }

            // create index on immutable table. here, orders table is immutable as we assume
            // we don't update any existing rows in the table.
            final String indexName = "CUSTOMER_ORDER_INDX";
            final String immutableIndexQuery = "CREATE INDEX CUSTOMER_ORDER_INDX ON ORDERS (CUST_ID, ORDER_ID) " +
                    " include (amount)";
            statement = this.connection.createStatement();
            statement.executeUpdate(immutableIndexQuery);

            // check if index is active.
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            resultSet = databaseMetaData.getTables(null, null, indexName, null);
            PIndexState indexState = null;
            while(resultSet.next()) {
                final String state = resultSet.getString("INDEX_STATE");
                indexState = PIndexState.valueOf(state);
            }

            System.out.println(String.format(" The index [%s] is currently [%s] ", indexName, indexState != null
                    ? indexState.name() : "unavailable"));

            resultSet = statement.executeQuery(amountQuery);
            while(resultSet.next()) {
                String explainPlan = resultSet.getString(1);
                System.out.println(explainPlan);
            }

            // lets insert data onto orders table
            // since ORDERS is an immutable table, we need to handle the failures effectively on the client itself.
            boolean retry = true;
            while(retry) {
                try {
                    statement.executeUpdate("UPSERT INTO ORDERS VALUES(10301,101,'1996-9-8',12000, 20)");
                    retry = false;
                } catch(SQLException sqle) {
                    // do an exponential backoff with limit.
                    log.error(sqle.getMessage(), sqle);
                }
            }

        } catch(Exception ex) {
            final String errorMsg = String.format("Error [%s] occurred while performing operations ", ex.getMessage());
            log.error(errorMsg, ex);
        } finally {
            close();
        }
    }
}
