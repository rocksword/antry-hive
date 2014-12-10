package com.an.antry.hive;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveOper {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveOper.class);
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive://10.103.218.27:10000/default";
    private Connection conn = null;

    public HiveOper() {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, "", "");
            LOGGER.info("Conn: {}", conn.toString());
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void query() throws UnknownHostException, SQLException {
        StringBuilder content = new StringBuilder();
        File f = new File("D:\\github\\antry-hive\\src\\xbl");
        FileLineIterator iter = new FileLineIterator(f.toString());
        String line = null;
        Statement st = null;
        try {
            int i = 0;
            st = conn.createStatement();
            while ((line = iter.nextLine()) != null) {
                if (line.startsWith("#") || line.startsWith(":")) {
                    continue;
                }
                InetAddress address = InetAddress.getByName(line);
                if (address.isSiteLocalAddress() || address.isLinkLocalAddress() || address.isLoopbackAddress()) {
                    continue;
                }
                String ip = address.getHostAddress();
                long iplong = ip2Long(ip);
                // System.out.println(iplong + ", " + ip);
                long t1 = System.currentTimeMillis();
                getLngLat(st, iplong);
                long t2 = System.currentTimeMillis();
                System.out.println(i + ", " + (t2 - t1));
                if (++i >= 100) {
                    break;
                }
            }
        } finally {
            iter.close();
            LOGGER.info("Close st.");
            st.close();
        }
    }

    private void getLngLat(Statement st, long iplong) throws SQLException {
        String sql = String.format(
                "SELECT latitude,longitude FROM blocksLocation WHERE startIpNum<=%s AND endIpNum>=%s", iplong, iplong);
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            double lat = rs.getDouble("latitude");
            double lng = rs.getDouble("longitude");
            // System.out.println(lat + " - " + lng);
        }
        if (rs != null) {
            rs.close();
        }
    }

    private long ip2Long(String ip) {
        long result = 0L;
        String[] ipArr = ip.split("\\.");
        for (int i = 3; i >= 0; i--) {
            long ipL = Long.parseLong(ipArr[3 - i]);
            result |= ipL << (i * 8);
        }
        return result;
    }

    /**
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void compute() throws SQLException, ClassNotFoundException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            // String sql = "select * from blocksLocation limit 5";
            long ip = 55555555l;
            String sql = String.format("SELECT * FROM blocksLocation WHERE startIpNum<=%s AND endIpNum>=%s", ip, ip);
            String sqlmin = String.format("SELECT min(startIpNum) FROM blocksLocation");
            String sqlmax = String.format("SELECT max(endIpNum) FROM blocksLocation");
            System.out.println(sql);
            exesql(stmt, sql);
            exesqlmin(stmt, sqlmin);
            exesqlmax(stmt, sqlmax);
        } finally {
            if (stmt != null) {
                LOGGER.info("Close stmt {}", stmt.toString());
                stmt.close();
            } else {
                LOGGER.warn("Null stmt.");
            }
        }
    }

    public void closeConn() {
        if (this.conn != null) {
            try {
                LOGGER.info("Close conn: {}", conn.toString());
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void exesqlmin(Statement stmt, String sqlmin) throws SQLException {
        ResultSet res = stmt.executeQuery(sqlmin);
        LOGGER.info("Execute select query result: ");
        while (res.next()) {
            try {
                long cnt = res.getLong(1);
                LOGGER.info("{}", cnt);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void exesqlmax(Statement stmt, String sqlmax) throws SQLException {
        ResultSet res = stmt.executeQuery(sqlmax);
        LOGGER.info("Execute select query result: ");
        while (res.next()) {
            try {
                long cnt = res.getLong(1);
                LOGGER.info("{}", cnt);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void exesql(Statement stmt, String sql) throws SQLException {
        ResultSet res = stmt.executeQuery(sql);
        LOGGER.info("Execute select query result: ");
        while (res.next()) {
            try {
                float lat = res.getFloat(4);
                float lng = res.getFloat(5);
                LOGGER.info("{} \t {} \t {}\t {}\t {}", res.getInt(1), res.getString(2), res.getString(3), lat, lng);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
