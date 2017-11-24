package com.jcinfo.crawler;

import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.plugin.ram.RamCrawler;
import cn.edu.hfut.dmic.webcollector.util.MD5Utils;
import com.jcinfo.jdbc.DBPool;
import com.jcinfo.util.CrawlerProperties;
import com.jcinfo.util.DBUtil;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Created by yanzhe on 2017/11/20.
 */
public class BaiduRSSCrawler extends RamCrawler {

    // 爬取个数
    private int crawlNum = CrawlerProperties.BAIDURSSNUM;
    // 存入数据库中的数据个数
    private int insertedNum;
    private HashSet<String> crawledLinks = new HashSet<String>();

    public int getAllVisitPage() {
        return allVisitPage;
    }

    public void addVisitPageNum(int num){
        allVisitPage += num ;
    }

    // 访问的所有页面数量
    private int allVisitPage ;

    public int getInsertedNum() {
        return insertedNum;
    }

    public int getCrawlNum() {
        return crawlNum;
    }

    public void setCrawlNum(int crawlNum) {
        this.crawlNum = crawlNum;
    }



    public BaiduRSSCrawler(String crawlPath, boolean autoParse){
        // 获取搜索关键字
        Map<Integer, String[]> keywordsMap = DBUtil.getKeywords();

        for(Map.Entry<Integer, String[]> entry : keywordsMap.entrySet()){
            int keywordId = entry.getKey();
            String[] keywords = entry.getValue();

            for (String v : keywords){
                // 百度RSS搜索链接
                String url = "http://news.baidu.com/ns?word="
                        + v + "&tn=newsrss&sr=0&cl=2&rn="
                        + crawlNum
                        +"&ct=0&newsid=" + keywordId;
                this.addSeed(url, "rss");
            }
        }

        this.addRegex("http://news.baidu.com/ns?word=*");

    }


    public void visit(Page page, CrawlDatums next) {

        String url = page.url();

        try{
            //如果匹配的是RSS页面
            if (page.matchType("rss")) {
                System.out.println("[info]:\t开始爬取RSS页面\t"+url);
                int newsIndex = url.indexOf("newsid=") + 7;
                int keywordId = Integer.parseInt(url.substring(newsIndex));

                //通过SAXBuilder解析xml
                SAXBuilder builder = new SAXBuilder();
                Document doc = builder.build(new ByteArrayInputStream(page.html().getBytes("gb2312")));
                Element root = doc.getRootElement();

                List<Element> items = root.getChild("channel").getChildren("item");

                addVisitPageNum(items.size());

                for (Element item : items) {
                    String title = item.getChild("title").getText();
                    String link = item.getChild("link").getText();
                    String uniq = MD5Utils.md5(link.getBytes());

                    System.out.println("[info]:\t文章信息提取中\t[title]\t" + title + "[link]\t" + link);


                    // 向数据库插入数据之前先进行重复判断
                    if(DBUtil.existLink(link)){
                        System.out.println("[info]:\t该新闻已经存在数据库中");
                        continue;
                    }else {
                        synchronized (DBPool.class){
                            if(crawledLinks.contains(uniq)){
                                System.out.println("[info]:\t该新闻已经被其他程序处理");
                                continue;
                            }
                            crawledLinks.add(uniq);
                        }
                    }


                    String description = item.getChild("description").getText();
                    String pubDate = item.getChild("pubDate").getText();
                    String source = item.getChild("source").getText();
                    String author = item.getChild("author").getText();

                    try{
                        HttpRequest request = new HttpRequest(link);
                        String html = request.response().decode();
                        if (html == null || html.equals("")){
                            System.out.println("[error]:\t新闻页面提取失败:\t" + link);
                            continue;
                        }
                        String text = ContentExtractor.getContentByHtml(html,link);


                        //向news表中插入信息
                        boolean inserted = DBUtil.insertInfo2Db(keywordId, title, link, description, pubDate, source, author, html, text);
                        if (inserted)
                            this.insertedNum++ ;

                    }catch (Exception e){
                        System.out.println("[error]:\t数据库插入失败:\t" + link);
                        e.printStackTrace();
                    }

                }
            }
        }catch (IOException e){
            System.out.println("[error]:\t页面抓取失败...");
            e.printStackTrace();
        } catch (JDOMException e) {
            System.out.println("[error]:\t xml解析错误...");
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

//        try {
//            // 如果匹配的是新闻详细页面
//            if (page.matchType("news")) {
//                System.out.println("------开始爬取新闻页面------");
//                String rawHtml = page.html();
//                String arg1 = Character.toString('\"');
//                String arg2 = "\\\\" + '"';
//                String targetHtml = rawHtml.replaceAll(arg1, arg2);
//                String arg3 = Character.toString('\'');
//                String arg4 = "\\\\'";
//                targetHtml = targetHtml.replaceAll(arg3, arg4);
//
//                News news = ContentExtractor.getNewsByUrl(url);
//                String text = news.getContent();
//
//                // 修改news表中信息
//                DBUtil.updateNews(url,targetHtml,text);
//            }
//        } catch (Exception e) {
//            // 如果失败应该删除表中信息(可以考虑通过多线程的方式删除)
//            DBUtil.deleteInfoByLink(url);
//            e.printStackTrace();
//        }
    }
}
