package com.jcinfo.jdbc;

import com.jcinfo.contentextractor.NewsExtractor;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yanzhe on 2017/11/20.
 */
public class DBPoolTest {
    @Test
    public void testProperties(){
        try {
            Properties prop = new Properties();
            InputStream in = DBPool.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(in);
//            dataSource.setDriverClass(prop.getProperty("jdbcdriver"));
//            dataSource.setJdbcUrl(prop.getProperty("url"));
//            dataSource.setUser(prop.getProperty("username"));
//            dataSource.setPassword(prop.getProperty("password"));

            String driver = prop.getProperty("jdbcdriver") ;
            String url = prop.getProperty("url");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNewsExtractor(){

        NewsExtractor extractor = new NewsExtractor();
        extractor.insertErrorInfo(1,"http://www.baidu.com",0);
        extractor.deleteErrorInfo(1);
        System.out.println();
    }
}
