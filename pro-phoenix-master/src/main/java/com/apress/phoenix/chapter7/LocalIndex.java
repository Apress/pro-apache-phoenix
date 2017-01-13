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
 * local index.
 */
@Slf4j
public class LocalIndex extends Application {

    public LocalIndex(Configuration configuration)  {
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

        // pre 4.8. add the below.
        //configuration.set("hbase.master.loadbalancer.class", "org.apache.phoenix.hbase.index.balancer.IndexLoadBalancer");
        //configuration.set("hbase.coprocessor.regionserver.classes", "org.apache.hadoop.hbase.regionserver.LocalIndexMerger");
        //configuration.set("hbase.coprocessor.master.classes", "org.apache.phoenix.hbase.index.master.IndexMasterObserver");

        new LocalIndex(configuration).doIt();
    }

    public void doIt() {
        try {
            start();
            final String createQuery = Queries.sql("chapter7/item-create.sql");
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            // upsert rows
            statement.executeUpdate("UPSERT INTO ITEM VALUES ('A1000','CH001','Clothing',20.28,8249)");
            statement.executeUpdate("UPSERT INTO ITEM VALUES('A1100','CH002','Daily Fresh',12.03,8251)");
            statement.executeUpdate("UPSERT INTO ITEM VALUES('A1200','CH003','Food',50,8333)");

            this.connection.commit();

            // create local on mutable table.
            final String localIndexQuery = "CREATE LOCAL INDEX item_supplier_indx ON ITEM (supplier_id)";
            statement = this.connection.createStatement();
            statement.executeUpdate(localIndexQuery);

            final DatabaseMetaData databaseMetaData = this.connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, "ITEM_SUPPLIER_INDX", null);
            while (resultSet.next()) {
                System.out.println(resultSet.getString("INDEX_TYPE"));
            }
        } catch(Exception ex) {
            final String errorMsg = String.format("Error [%s] occurred while performing operations ", ex.getMessage());
            log.error(errorMsg, ex);
        } finally {
            close();
        }
    }
}
