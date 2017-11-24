package com.jcinfo.util;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import java.io.FileOutputStream;


public class XmlUtil {

    /**
     * @param usetime 一次爬虫任务用时
     * @param startime 爬虫任务开始时间
     * @param endTime 爬虫任务结束时间
     * @param allCrawledNum 爬取的所有页面数量
     * @param successedNum 成功入库的记录数量
     */
    public static void generateXml(String usetime, String startime, String endTime,String allCrawledNum,String successedNum){
        try {
            // 通过SAXBuilder解析xml
            SAXBuilder builder = new SAXBuilder();

            Document doc = builder.build(XmlUtil.class.getClassLoader().getResourceAsStream("crawlerlog.xml"));
            Element root = doc.getRootElement();
            Element info = new Element("info");

            // 用时
            info.setAttribute("usetime", usetime);
            // 开始时间
            info.setAttribute("starttime", startime);
            // 结束时间
            info.setAttribute("endtime", endTime);
            // 总共解析的页面
            info.setAttribute("allcrawlednum",allCrawledNum);
            // 成功入库的数量
            info.setAttribute("successednum",successedNum);

            root.addContent(info);

            XMLOutputter out = new XMLOutputter();
            Format format = Format.getPrettyFormat();
            format.setIndent("    ");
            out.setFormat(format);
            String file = XmlUtil.class.getClassLoader().getResources("crawlerlog.xml").nextElement().getFile();

            out.output(doc, new FileOutputStream(file));

            System.out.println("[success]:\t log日志记录成功~~~");
        } catch (Exception e) {
            System.out.println("[success]:\t log日志记录成功~~~");
            e.printStackTrace();
        }


    }

}
