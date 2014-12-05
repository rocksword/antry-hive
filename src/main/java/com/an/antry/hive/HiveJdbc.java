package com.an.antry.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJdbc {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveJdbc.class);
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive://10.10.10.10:10000/default";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            stmt = conn.createStatement();
            String sql = "select * from blocksLocation limit 5";
            ResultSet res = stmt.executeQuery(sql);
            LOGGER.info("Execute select query result: ");
            while (res.next()) {
                LOGGER.info("{} \t {} \t {}", res.getInt(1), res.getString(2), res.getString(3));
            }
        } finally {
            if (conn != null) {
                LOGGER.info("Close conn {}", conn.toString());
                conn.close();
            } else {
                LOGGER.warn("Null conn.");
            }
            if (stmt != null) {
                LOGGER.info("Close stmt {}", stmt.toString());
                stmt.close();
            } else {
                LOGGER.warn("Null stmt.");
            }
        }
    }

    private static Connection getConn() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, "", "");
        LOGGER.info("Connection {}", conn.toString());
        LOGGER.info("getCatalog {}", conn.getCatalog());
        return conn;
    }
}
