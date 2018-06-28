package com.kafka.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Administrator on 2017/9/4.
 */
public class DatabaseUtil {
    /**
     *
     */
    public static Connection getOracleConn() throws ClassNotFoundException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String url = ConfigUtil.getValByKey("db.url");
        String user = ConfigUtil.getValByKey("db.user");
        String password = ConfigUtil.getValByKey("db.password");
        java.sql.Connection conn = null;
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void colseConn(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
