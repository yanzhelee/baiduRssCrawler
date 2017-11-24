package com.jcinfo.contentextractor;

import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import cn.edu.hfut.dmic.contentextractor.News;
import com.jcinfo.util.CrawlerProperties;
import com.jcinfo.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author :yanzhelee
 * 正文提取
 */
public class NewsExtractor {

    private int newsCnt = 0;           // 用于统计新闻总数
    private int updateNewsCnt = 0;     // 用于累计更新的新闻数量
    private int failUpdateNewsCnt = 0; // 用于累计更新失败的新闻数量

    /**
     * 正文提取函数
     */
    public void extractContent() {

        // 程序起始时间
        long current = System.currentTimeMillis();
        System.out.println("程序开始时间为:" + current);
        Connection conn = null ;
        try{
            conn = DBUtil.getConnection();

            // 查询数据库中新闻总数
            ResultSet newCountRs = conn.prepareStatement("select count(1) from " + CrawlerProperties.TABLE_NEWS).executeQuery();
            if(newCountRs.next()){
                newsCnt = newCountRs.getInt(1);
                System.out.println("新闻总数为" + newsCnt);
            }

            // 将数据库中的所有链接查询出来
            PreparedStatement linkPpst = conn.prepareStatement("select id, link from " + CrawlerProperties.TABLE_NEWS+ " WHERE id > 0") ;
            ResultSet rs = linkPpst.executeQuery();

            // 遍历链接
            while (rs.next()){
                String link = rs.getString("link");
                int id = rs.getInt("id");

                try{// 提取新闻正文，并更新数据库
                    News news = ContentExtractor.getNewsByUrl(link);
                    String context = news.getContent();
                    PreparedStatement updateText = conn.prepareStatement("UPDATE "+ CrawlerProperties.TABLE_NEWS +" SET text = ? WHERE id = ?");
                    updateText.setString(1, context);
                    updateText.setInt(2, id);

                    int cnt = updateText.executeUpdate();
                    if (cnt > 0){
                        updateNewsCnt ++ ;
                        System.out.println("-----------------------------------------");
                        System.out.println("用时:" + ((System.currentTimeMillis() - current)/60000) + "分");
                        System.out.println("当前修改的新闻id为:" + id);
                        System.out.println("[success] 新闻总数:" + newsCnt + "\t已修改数量:" + updateNewsCnt + "\t修改失败数量:" + failUpdateNewsCnt);
                        System.out.println("-----------------------------------------");

                    }

                }catch (SQLException e){

                    System.out.println("插入数据库失败,链接为:" + link);
                    e.printStackTrace();
                    failUpdateNewsCnt ++ ;
                    insertErrorInfo(id,link,0);
                }catch (Exception e){

                    System.out.println("正文解析失败,链接为:" + link);
                    e.printStackTrace();
                    failUpdateNewsCnt++;
                    // 将错误信息插入数据库
                    insertErrorInfo(id,link,0);
                }
            }
        } catch (SQLException e){
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
        System.out.println("程序结束时间为:" + ((System.currentTimeMillis() - current)/60000) + "分钟");
    }


    /**
     * 正文提取函数
     */
    public void extractContent(ResultSet rs) {

        Connection conn = null;
        try{
            conn = DBUtil.getConnection();
            // 遍历链接
            while (rs.next()){
                String link = rs.getString("link");
                int id = rs.getInt("id");

                try{// 提取新闻正文，并更新数据库
                    News news = ContentExtractor.getNewsByUrl(link);
                    String context = news.getContent();
                    PreparedStatement updateText = conn.prepareStatement("UPDATE "+ CrawlerProperties.TABLE_NEWS+" SET text = ? WHERE id = ?");
                    updateText.setString(1, context);
                    updateText.setInt(2, id);

                    int cnt = updateText.executeUpdate();
                    if (cnt > 0){
                        deleteErrorInfo(id);
                        updateNewsCnt ++ ;
                    }

                }catch (SQLException e){

                    System.out.println("插入数据库失败,链接为:" + link);
                    e.printStackTrace();
                    failUpdateNewsCnt ++ ;
                    insertErrorInfo(id,link,0);
                }catch (Exception e){

                    System.out.println("正文解析失败,链接为:" + link);
                    e.printStackTrace();
                    failUpdateNewsCnt++;
                    // 将错误信息插入数据库
                    insertErrorInfo(id,link,0);
                }

            }
        } catch (SQLException e){
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
     * 该函数用于向error表中添加错误信息数据
     * @param newsId 新闻id
     * @param link 新闻链接
     * @param errorType 出错类型，数值0表示数据库更新失败，数值1表示正文解析失败
     *
     */
    public void insertErrorInfo(int newsId, String link, int errorType){
        Connection conn = null;
        try {
            conn = DBUtil.getConnection();

            PreparedStatement ppstIdCnt = conn.prepareStatement("SELECT newsId FROM error WHERE newsId = ?");
            ppstIdCnt.setInt(1,newsId);
            // 如果数据库中没有值，则向数据库中插入信息
            if(!ppstIdCnt.executeQuery().next()){
                PreparedStatement ppstInsertError = conn.prepareStatement("INSERT INTO error VALUES (?,?,?)");
                ppstInsertError.setInt(1,newsId);
                ppstInsertError.setString(2,link);
                ppstInsertError.setInt(3,errorType);

                if(ppstInsertError.execute()){// 错误信息插入成功

                    System.out.println("错误信息插入成功！！！");
                }
            }
        } catch (SQLException e) {
            System.out.println("错误日志数据插入失败");
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
     * 该函数用于向error表中删除错误信息数据
     * @param newsId 新闻id
     *
     */
    public void deleteErrorInfo(int newsId){
        Connection conn = null;
        try {

            conn = DBUtil.getConnection();

            PreparedStatement ppst = conn.prepareStatement("DELETE FROM error WHERE newsId = ?");
            ppst.setInt(1,newsId);
            ppst.execute();
            System.out.println("错误日志数据删除成功！！！");

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("错误日志数据删除失败");
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

}
