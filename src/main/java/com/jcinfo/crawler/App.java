package com.jcinfo.crawler;

import com.jcinfo.util.CrawlerProperties;
import com.jcinfo.util.XmlUtil;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by yanzhe on 2017/11/22.
 */
public class App {
    public static void main(String[] args) {
        Timer timer = new Timer();
        // 定时任务每个1小时执行一次
        timer.schedule(new TimerTask() {
            public void run() {
                startCrawler();
            }
        }, 0, 1000*60*60* CrawlerProperties.SCHEDULERPERIOD);

        //startCrawler();

    }

    public static void startCrawler(){
        System.out.println("---------爬虫程序启动！！！-------------");

        // 开始时间
        long startTime = System.currentTimeMillis()/1000;
        BaiduRSSCrawler crawler = new BaiduRSSCrawler("baiduRssCrawl", true);
        crawler.setThreads(100);
        crawler.getConf().setTopN(100);
        crawler.setResumable(true);
        //开启爬虫
        try {
            crawler.start(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 结束时间
        long endTime = System.currentTimeMillis()/1000;
        // 用时
        long useTime = endTime - startTime ;
        String allVisitPage = crawler.getAllVisitPage()+"";
        String insertedNum = crawler.getInsertedNum()+"";

        XmlUtil.generateXml(useTime+"",startTime+"",endTime+"",allVisitPage, insertedNum);

        System.out.println("--------------------------------------");
        System.out.println("抓取结束,共抓取：\t" + allVisitPage +" 其中成功入库的数量为：\t" + insertedNum);
        System.out.println("用时：" + useTime + "秒");
        System.out.println("--------------------------------------");
    }
}
