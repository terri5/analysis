/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hangmei.model;

import com.hangmei.util.ConvertUtil;
import com.hangmei.util.RegexUtil;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author terri
 */
public class PV implements IHBaseModel {

    public static String HTTP_URI = "httpUri";
    static String DATETIME_POINT = "DateTime_Point";
    public static String TABLE_NAME = "DEVICE_LOG_PV";
    public static String COLUMN_FAMILY = "PV";
    public static String DMAC = "dmac";
    public static String MAC = "mac";
    public static String CLIENT_AGENT = "clientAgent";
    public static String HTTP_METHOD = "httpMethod";
    public static String HTTP_VERSION = "httpVersion";
    public static String HTTP_HOST = "httpHost";
    public static String IP = "ip";
    public static String DAY_ID = "day_id";
    public static String IN_DB_DATETIME = "INDB_DATETIME";
    public static String REFERER = "referer";
    public static String CLIENT_OS = "client_os";
    public static String MOBILE_BRAND = "mobile_brand";
    public static String STATUS = "status";
    public static String BODY_BYTES_SENT = "body_bytes_sent";

    public static String CLIENT_BROWSER = "client_browser";
    private static final int BATCH = 20000;

    ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<>();
    ReadWriteLock rwl = new ReentrantReadWriteLock();

    @Override
    public Map<String, String> LineToMap(String line, String dmac) {
        String ip = RegexUtil.getSingleIpString(line);
        if (StringUtils.isEmpty(ip)) { //没有找到符合条件的IP地址
            //System.err.println("error:ip:" + line);
            return null;
        }
        //    System.out.println("获取ip:"+line);
        String mac = "";
        if (ip.contains("+")) {
            String[] ss = ip.split("\\+");
            ip = ss[0].trim();
            if (ss.length < 2 && !line.contains("hitID")) {
                return null;  //没有mac，也不是hit
            }
            mac = ss[1];
        } else {
            if (!line.contains("hitID"))//不是hit
            {
                return null;
            }
        }

        ZoneId zone = ZoneId.of("Asia/Shanghai");
        ZonedDateTime hostdate = Instant.now().atZone(zone);
        String dateStr = RegexUtil.getDateTimeString(line);

        if (StringUtils.isNotEmpty(dateStr)) {
            dateStr = dateStr.substring(0, dateStr.length() - 6); //去掉文本行中的时区标示信息
            SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
            Date date = null;
            try {
                date = f.parse(dateStr);
            } catch (ParseException ex) {
                Logger.getLogger(PV.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
            if (!ConvertUtil.Validate(date)) {
                return null;
            }

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map<String, String> obj = new HashMap<>();
            dmac = ConvertUtil.FormatMacString(dmac);
            obj.put("dmac", dmac);
            obj.put("mac", mac);
            obj.put("ip", ip);
            obj.put(DATETIME_POINT, sdf.format(date).replace("[-| |:]", ""));
            obj.put("day_id", sdf.format(date).substring(0, 10).replace("-", ""));
            List<String> strs = RegexUtil.GetStringsByRegex(RegexUtil.QUOTES_EX, line);
            boolean blnUrl = false, blnRequest = false;
            String url = "", httpUri = "", httpMethod = "", httpVer = "", httpHost = "";

            for (String str : strs) {
                str = str.replaceAll("\"", "").trim();

                if (str.matches(RegexUtil.URL_EXP)) {
                    url = str;
                    blnUrl = true;
                }
                //    System.out.println(str.contains(" ") && str.substring(str.indexOf(" ")+1).matches(RegexUtil.REQUEST_EX)
                //            && str.substring(0,str.indexOf(" ")).matches(RegexUtil.HTTP_HOST_EXP));
                if (str.contains(" ") && str.substring(str.indexOf(" ") + 1).matches(RegexUtil.REQUEST_EX)
                        && str.substring(0, str.indexOf(" ")).matches(RegexUtil.HTTP_HOST_EXP)) {
                    Matcher m = RegexUtil.P_HTTP_HOST.matcher(str);
                    String host = "";
                    if (m.find()) {
                        host = m.group(0).trim();
                        if (!host.matches("^(GET|POST|HEAD|OPTIONS|PUT|-)$")) {
                            httpHost = host.trim();
                        }

                    }

                    Matcher mt = RegexUtil.P_HTTP_METHOD.matcher(str);
                    String m2 = "", v = "";
                    if (mt.find()) {
                        m2 = mt.group();
                        httpMethod = m2.trim();
                        Matcher mt3 = RegexUtil.P_HTTP_PROTOCAL.matcher(str);
                        if (mt3.find()) {
                            v = mt3.group();
                            httpVer = v.trim();
                        }
                    }
                    httpUri = str.replaceAll(m2, "").replaceAll(v, "").replaceAll(host, "").trim();

                    blnRequest = true;
                }
            }
            String status = "";
            String body_bytes_sent = "";
            String httpstatus = RegexUtil.GetHttpStatusAndBodySent(line.substring(60));
            if (httpstatus != null) {
                String[] str = httpstatus.split(" ");
                status = str[0];
                body_bytes_sent = str[1];
            }

            if (!blnUrl && !blnRequest) {
                //    System.err.println(LocalTime.now()+"err line: "+line);
                return null;
            }

            if (strs.size() > 2) {
                obj.put("clientAgent", strs.get(2).replaceAll("\"", ""));
                obj.putAll(ConvertUtil.getClientInfo(strs.get(2)));
            }

            if (StringUtils.isEmpty(httpUri) && StringUtils.isEmpty(url) && !line.contains("hitID")) {
                return null;
            }
            // if(url=="") System.err.println("error reffer:"+line);
            obj.put(BODY_BYTES_SENT, body_bytes_sent);
            obj.put(STATUS, status);
            obj.put(HTTP_URI, httpUri);
            obj.put(HTTP_METHOD, httpMethod);
            obj.put(HTTP_VERSION, httpVer);
            obj.put(HTTP_HOST, httpHost);
            obj.put(REFERER, url);
            obj.put(IN_DB_DATETIME, hostdate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            String rowkey = getRowKey(obj, date);
            obj.put(ROWKEY, rowkey);
            return obj;
        }
        return null;
    }

    @Override
    public String GetColumnFamily() {
        return COLUMN_FAMILY;
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public void addMap(Map<String, String> data) {
        rwl.readLock().lock();
        try {
            queue.add(data);
        } finally {
            rwl.readLock().unlock();
        }

    }

    @Override
    public boolean shouldIntoHbase() {
        rwl.readLock().lock();
        try {
            return queue.size() >= BATCH;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override
    public ConcurrentLinkedQueue<Map<String, String>> GetAndResetCacheQueueData() {
        ConcurrentLinkedQueue<Map<String, String>> tmp = null;
        rwl.writeLock().lock();
        try {
            tmp = queue;
            queue = new ConcurrentLinkedQueue<>();
            return tmp;
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override
    public String getRowKey(Map<String, String> obj, Date date) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddssHHmm");
            return sdf.format(date) + ConvertUtil.FormatMacString(obj.get(MAC)) + obj.get(DMAC)
                    + ConvertUtil.EncodeByMd5(obj.get(HTTP_URI) + obj.get(REFERER)).substring(0, 8);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            Logger.getLogger(PV.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
