package com.jcinfo.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by yanzhe on 2017/11/20.
 */
public class DBPool {
    private static DBPool instance ;
    private ComboPooledDataSource dataSource ;

    static {
        instance = new DBPool();
    }

    private DBPool(){
        try {
            dataSource = new ComboPooledDataSource();
            Properties prop = new Properties();
            InputStream in = DBPool.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(in);
            dataSource.setDriverClass(prop.getProperty("jdbcdriver"));
            dataSource.setJdbcUrl(prop.getProperty("url"));
            dataSource.setUser(prop.getProperty("username"));
            dataSource.setPassword(prop.getProperty("password"));
            dataSource.setMaxPoolSize(100);      // 最大连接数
            dataSource.setMinPoolSize(20);       // 最小连接数
            dataSource.setInitialPoolSize(20);   // 初始化连接数
            dataSource.setAcquireIncrement(5);  // 增量

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static DBPool getInstance(){
        return instance;
    }

    // 获取数据库链接
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


}
