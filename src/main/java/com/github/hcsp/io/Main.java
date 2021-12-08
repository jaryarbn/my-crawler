package com.github.hcsp.io;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {

    private static List<String> loadUrlsFromDatabase(Connection connection, String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                results.add(resultSet.getString(1));
            }
        }
        return results;
    }

    public static void main(String[] args) throws IOException, SQLException {
        Connection connection = DriverManager.getConnection("jdbc:h2:file:/Users/jyb/Desktop/hcsp-projects/my-crawler/news","root","root");

        //待处理的链接池
        // 从数据库加载即将处理的链接的代码
        List<String> linkPool = loadUrlsFromDatabase(connection, "select link from LINKS_TO_BE_PROCESSED");


        //已经处理的链接池
        // 从数据库加载已经处理的链接的代码
        Set<String> processedLinks = new HashSet<>(loadUrlsFromDatabase(connection, "select link from LINKS_ALREADY_PROCESSED"));
        try {
            while (true) {
                if (linkPool.isEmpty()) {
                    break;
                }
                String link = linkPool.remove(linkPool.size() - 1);
                //ArrayList从尾部删除更有效率
                // 每次处理完后，更新数据库
                if (processedLinks.contains(link)) {
                    continue;
                }
                if (isInterestingLink(link)) {
                    Document doc = httpGetAndParseHtml(link);
                    doc.select("a").stream().map(aTag -> aTag.attr("href")).forEach(linkPool::add);
                    //假如这是一个新闻的详情页面，就存入数据库，否则，就什么都不做
                    storeIntoDatabaseIfItIsNewsPage(doc);
                    processedLinks.add(link);
                } else {
                    //这是我们不感兴趣的。不处理它
                }
            }
        } finally {
            System.out.println("Exit");
        }
    }


    private static void storeIntoDatabaseIfItIsNewsPage(Document doc) {
        ArrayList<Element> articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTags.get(0).child(0).text();
                System.out.println(title);
            }
        }
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification = "Document why this should be ignored here")
    private static Document httpGetAndParseHtml(String link) throws IOException {

        //这是我们感兴趣的，我们只处理新浪站内的链接
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36");
        if (link.startsWith("//")) {
            link = "https:" + link;
        }
        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
            System.out.println(link);
            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }

    private static boolean isInterestingLink(String link) {
        return isNotLoginPage(link) && isNewsPage(link) || isIndexPage(link);
    }

    private static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }
}


