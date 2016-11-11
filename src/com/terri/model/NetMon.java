/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.model;

import com.terri.util.ConvertUtil;
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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

/**
 *
 * @author terri
 */
public class NetMon implements IHBaseModel {

    private static final String TABLE_NAME = "DEVICE_LOG_PV";
    private static final String COLUMN_FAMILY = "PV";
    private static final String DMAC = "dmac";
    private static final String MAC = "mac";
    private static final String HTTP_URI = "httpUri";
    private static final String HTTP_HOST = "httpHost";
    private static final String HTTP_METHOD = "httpMethod";
    private static final String IP = "ip";
    private static final String DATETIME_POINT = "DateTime_Point";
    private static final String DAY_ID = "day_id";
    private static final String IN_DB_DATETIME = "INDB_DATETIME";
    private static final int BATCH = 2000;
    ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<>();
    ReadWriteLock rwl = new ReentrantReadWriteLock();

    @Override
    public String GetColumnFamily() {
        return COLUMN_FAMILY;
    }

    public String GetTableName() {
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
    public Map<String, String> LineToMap(String line, String dmac) {

        if (StringUtils.isEmpty(line)) {
            return null;
        }
        String[] fields = line.replaceAll(" {2,}", " ").split(" ");

        Map<String, String> bson;
        bson = new HashMap<>();

        int ix, jx;
        // 把非空的字段集中到一起

        int fieldCount;
        for (fieldCount = 0; fieldCount < fields.length && StringUtils.isNoneBlank(fields[fieldCount]); fieldCount++) ;
        if (fieldCount != 8 && fieldCount != 9 || !fields[5].equals(">")) {
            return null;
        }

        Date date;
        try {
            date = DateUtils.parseDate(fields[0] + " " + fields[1], "yyyy-MM-dd HH:mm:ss");
        } catch (ParseException ex) {
            Logger.getLogger(NetMon.class.getName()).log(Level.SEVERE, "erro line for get date: \n" + line, ex);
            return null;
        }

        if (date == null) {
            return null;
        }

        String umac = fields[2];
        if (umac.length() == 16) {
            umac = "0" + umac;
        }
        if (umac.length() != 17) {
            return null;
        }
        umac = umac.toLowerCase();

        // 分析URI
        String uri = "";
        String host = "";

        if (fieldCount == 9) {
            host = fields[7];
            uri = fields[8];
            if (uri.startsWith("http://")) {
                uri = uri.substring(7);
            }
            if (uri.startsWith(host)) {
                uri = uri.substring(host.length());
            }
        } else {
                                                    uri = fields[7];
                                                    ix = uri.indexOf("/");
                                                    if (ix < 0) {
                                                        return null;
                                                    }

                                                    host = uri.substring(0, ix);
                                                    if (!host.contains(".")) {
                                                                                   host = StringUtils.EMPTY;
                                                    }
                                                    uri = uri.substring(ix);
        }
        if (StringUtils.isEmpty(uri)) {
                                 return null;
        }
        String ip = fields[3];
        String httpMethod = fields[6];
        if (!ConvertUtil.Validate(date))         return null;   //过滤掉15天前的数据
            ZoneId zone = ZoneId.of("Asia/Shanghai");
       ZonedDateTime   hostdate =Instant.now().atZone(zone);
         SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        bson.put(DATETIME_POINT, sdf.format(date).replace("[-| |:]", ""));
        bson.put(DAY_ID, sdf.format(date).substring(0,10).replace("-", ""));
        bson.put(HTTP_HOST, host);
        bson.put(HTTP_URI, uri);
        bson.put(MAC, umac);
        bson.put(IP, ip);
        bson.put(HTTP_METHOD, httpMethod);
        bson.put(DMAC, dmac);
        bson.put(IN_DB_DATETIME, hostdate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
        String rowkey = getRowKey(bson, date);
        bson.put(ROWKEY, rowkey);
        return bson;

    }

    @Override
    public String getRowKey(Map<String, String> obj, Date date) {
                    SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddssHHmm");
                try {
                    return sdf.format(date)+ ConvertUtil.FormatMacString(obj.get(MAC)) + obj.get(DMAC)  +
                            ( ConvertUtil.EncodeByMd5(obj.get(HTTP_URI)).substring(0,8));
                } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
                    Logger.getLogger(NetMon.class.getName()).log(Level.SEVERE, null, ex);
                }
                return null;
    }


    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

}
