package com.apress.phoenix.common;

import org.apache.phoenix.util.QueryUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 */
public class ConnectionUtil {

    private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final int ZOOKEEPER_PORT = 2181;
    private static final Properties EMPTY = new Properties();

    public static Connection getConnection(int zookeeperPort) throws SQLException {
        return getConnection(ZOOKEEPER_HOST, zookeeperPort);
    }

    public static Connection getConnection(String zookeeperHost, int zookeeperPort) throws SQLException {
        return getConnection(zookeeperHost, zookeeperPort, EMPTY);
    }

    public static Connection getConnection(String zookeeperHost, int zookeeperPort, Properties properties) throws SQLException {
        final String url = QueryUtil.getUrl(zookeeperHost, zookeeperPort);
        Connection connection = DriverManager.getConnection(url, properties);
        return connection;
    }




}
