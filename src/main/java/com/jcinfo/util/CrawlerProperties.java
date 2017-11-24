package com.jcinfo.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yanzhe on 2017/11/23.
 */
public class CrawlerProperties {
    // baidunews数据库中的数据表
    public static String TABLE_NEWS;
    public static String TABLE_KEYWORDS;
    public static String TABLE_NEWS_KEYWORD;
    public static int BAIDURSSNUM;
    public static int SCHEDULERPERIOD;

    // 加载数据表配置信息
    static {
        try {
            System.out.println("----------------------------");
            System.out.println("[info]:加载数据表配置信息");
            System.out.println("----------------------------");
            Properties prop = new Properties();
            InputStream in = DBUtil.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(in);
            TABLE_NEWS = prop.getProperty("table_news");
            TABLE_KEYWORDS = prop.getProperty("table_keywords");
            TABLE_NEWS_KEYWORD = prop.getProperty("table_news_keyword");
            BAIDURSSNUM = Integer.parseInt(prop.getProperty("baidurssnum"));
            SCHEDULERPERIOD = Integer.parseInt(prop.getProperty("schedulerperiod"));

        } catch (IOException e) {
            System.out.println("----------------------------");
            System.out.println("[error]:配置信息出错");
            System.out.println("----------------------------");
            e.printStackTrace();
        }
    }
}
