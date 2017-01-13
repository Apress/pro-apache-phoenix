package com.apress.phoenix.common;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import java.io.Closeable;
import java.sql.Connection;

/**
 * @author team
 */
public class Application implements Closeable {

    private final HBaseTestingUtility utility;
    protected Connection connection;

    public Application() {
        this(HBaseConfiguration.create());
    }

    public Application(final Configuration configuration) {
        utility = new HBaseTestingUtility(configuration);
    }

    public void start() throws Exception {
        utility.startMiniCluster();
        int zookeeperPort = utility.getZkCluster().getClientPort();
        connection = ConnectionUtil.getConnection(zookeeperPort);
    }

    public void close() {
        if(utility != null) {
            try {
                this.connection.close();
                utility.shutdownMiniCluster();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    }
}
