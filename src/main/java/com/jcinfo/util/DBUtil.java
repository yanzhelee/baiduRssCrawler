package com.jcinfo.util;
import cn.edu.hfut.dmic.webcollector.util.MD5Utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.jcinfo.jdbc.DBPool;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by yanzhe on 2017/11/20.
 */
public class DBUtil {

    /**
     * 获取数据库连接
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        DBPool pool = DBPool.getInstance();
        return pool.getConnection();
    }

    /**
     * 获取搜索关键字信息
     *
     * @return 返回关键字的map集合，map的key为关键字id，value为该id下的所有关键字数组
     */
    public static Map<Integer, String[]> getKeywords() {

        Connection conn = null ;
        Map<Integer, String[]> map = new HashMap<Integer, String[]>();

        try {
            conn = DBUtil.getConnection();
            // 提取数目小于1000的关键词，因为条目数量大于1000将不再进行统计
            PreparedStatement ppst = conn.prepareStatement("SELECT id, keyword, synonyms FROM " + CrawlerProperties.TABLE_KEYWORDS+" WHERE dataCount<1000");
            ResultSet rs = ppst.executeQuery();

            while (rs.next()) {
                int keywordId = rs.getInt("id");
                String keyword = rs.getString("keyword");
                String synonyms = rs.getString("synonyms");
                // 解析json字符串
                JSONArray jsList = JSON.parseArray(synonyms);

                //将关键字都存入values中
                String[] values = new String[jsList.size() + 1];
                values[0] = keyword;
                for (int i = 0; i < jsList.size(); i++) {
                    values[i + 1] = jsList.get(i).toString();
                }

                map.put(keywordId, values);
            }

        } catch (SQLException e) {
            System.out.println("[error]:\t关键字提取失败");
            e.printStackTrace();
        }finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return map;
    }


    /**
     * 向数据表news，keywords，news_keyword插入数据
     * @param keywordId
     * @param title
     * @param link
     * @param description
     * @param pubDate
     * @param source
     * @param author
     * @param html
     * @param text
     */
    public static boolean insertInfo2Db(int keywordId, String title, String link,
                                     String description, String pubDate, String source, String author,String html, String text){

        Connection conn = null ;
        try {
            conn = DBUtil.getConnection();
            conn.setAutoCommit(false); // 设置自动提交

            // 1.更新keywords表
            PreparedStatement ppst = conn.prepareStatement("UPDATE "+ CrawlerProperties.TABLE_KEYWORDS+" set dataCount=dataCount+1 where id=?");
            ppst.setInt(1,keywordId);
            ppst.execute();

            // 2.向news表插入数据
            String uniq = MD5Utils.md5(link.getBytes());
            int created = (int) (System.currentTimeMillis() / 1000);
            String sql = "INSERT INTO "+ CrawlerProperties.TABLE_NEWS+"(uniq,title,link,description,pubDate,source,author,created,html,text) VALUES (?,?,?,?,?,?,?,?,?,?)";
            ppst = conn.prepareStatement(sql);
            ppst.setString(1, uniq);
            ppst.setString(2, title);
            ppst.setString(3, link);
            ppst.setString(4, description);
            ppst.setString(5, pubDate);
            ppst.setString(6, source);
            ppst.setString(7, author);
            ppst.setInt(8, created);
            ppst.setString(9,html);
            ppst.setString(10,text);
            ppst.execute();

            // 3.向news_keyword表插入数据
            ppst = conn.prepareStatement("SELECT id FROM "+ CrawlerProperties.TABLE_NEWS+" WHERE link = ?"); // 获取新闻id
            ppst.setString(1,link);
            ResultSet rs = ppst.executeQuery();
            if(rs.next()){
                int newsId = rs.getInt("id");
                ppst = conn.prepareStatement("INSERT INTO "+ CrawlerProperties.TABLE_NEWS_KEYWORD+" VALUES (?,?)");
                ppst.setInt(1, newsId);
                ppst.setInt(2, keywordId);
                ppst.execute();
            }

            conn.commit();
            System.out.println("[success]:\t数据库更新成功~~~");
            return true;

        } catch (Exception e) {
            System.out.println("[error]:\t数据库更新失败~~~");
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();

        }finally {
            if (conn != null){
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    /**
     * 修改数据表news，根据link找到对应数据行，并且将html信息和text信息加入到该行中
     * @param link
     * @param html
     * @param text
     */
    @Deprecated
    public static void updateNews(String link, String html, String text){
        Connection conn = null ;
        if (link == null){
            return;
        }else if (html == null || html.equals("") || text == null || text.equals("")){
            // 将数据库中所有关联数据删除
            deleteInfoByLink(link);
            return;
        }
        try {
            conn = DBUtil.getConnection();
            PreparedStatement ppst = conn.prepareStatement("UPDATE "+ CrawlerProperties.TABLE_NEWS+" SET html = ?, text = ? where link=?");
            ppst.setString(1,html);
            ppst.setString(2, text);
            ppst.setString(3, link);
            ppst.execute();
            System.out.println("正文提取成功");

        } catch (SQLException e) {
            // 将数据库中所有关联数据删除
            deleteInfoByLink(link);
            e.printStackTrace();
        }finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 通过link删除数据库中的新闻以及相应的关联信息等
     * @param link
     */
    public static void deleteInfoByLink(String link){
        Connection conn = null ;
        try {
            conn = DBUtil.getConnection();

            conn.setAutoCommit(false);

            // 1. 减少keywords对应数据
            PreparedStatement ppst = conn.prepareStatement("UPDATE "+ CrawlerProperties.TABLE_KEYWORDS+" SET dataCount=dataCount-1;");
            ppst.execute();

            // 2. 删除news表中对应行信息
            ppst = conn.prepareStatement("DELETE FROM "+ CrawlerProperties.TABLE_NEWS+" WHERE link=?;");
            ppst.setString(1, link);
            ppst.execute();

            // 3. 删除news_keyword关联信息
            ppst = conn.prepareStatement("DELETE FROM "+ CrawlerProperties.TABLE_NEWS_KEYWORD+" WHERE news IN (SELECT id FROM "+ CrawlerProperties.TABLE_NEWS+" WHERE link=?)");
            ppst.setString(1,link);
            ppst.execute();

            conn.commit();
            System.out.println("[success]:\t无效信息删除成功~~~");

        } catch (SQLException e) {
            System.out.println("[error]:\t信息删除失败~~~");
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }finally {
            if (conn != null){
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }


    }

    /**
     * 判断该连接在数据库中是否存在
     * @param link
     * @return
     */
    public static boolean existLink(String link) {
        Connection conn = null;
        // 用于记录数据库中对应link的数量
        int cnt = 0 ;
        try {
            conn = DBUtil.getConnection();
            PreparedStatement ppst = conn.prepareStatement("SELECT COUNT(1) FROM " + CrawlerProperties.TABLE_NEWS + " WHERE link=?;");
            ppst.setString(1,link);
            ResultSet rs = ppst.executeQuery();
            if(rs.next()){
                cnt = rs.getInt(1);
            }

            return (cnt > 0) ;

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // 之所以放回true就是为了最大程度的避免news表中没有重复数据
        return true;


    }
}
