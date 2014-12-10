package com.an.antry.hive;

import java.net.UnknownHostException;
import java.sql.SQLException;

public class HiveMain {
    public static void main(String[] args) {
        HiveOper oper = new HiveOper();
        try {
            oper.query();
        } catch (UnknownHostException | SQLException e) {
            e.printStackTrace();
        }
        oper.closeConn();
    }
}
