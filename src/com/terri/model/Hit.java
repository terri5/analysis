/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.model;

import com.terri.util.ConvertUtil;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author terri
 */
public class Hit implements IHBaseModel {

    public final static String TABLE_NAME = "DEVICE_LOG_HIT";
    public final static String COLUMN_FAMILY = "HIT";
    public final static String DMAC = "dmac";
    public final static String MAC = "mac";
    public final static String IP = "ip";
    public final static String TIME = "time";
    public final static String HITID = "hitID";
    public final static String REFHITID = "refHitID";
    public final static String UID = "uID";
    public final static String POSIDX = "posIdx";
    public final static String PAGETIME = "pageTime";
    public final static String DAY_ID = "day_id";
    public final static String INDB_DATETIME = "INDB_DATETIME";
    private static final int BATCH = 2000;

    public final static String USER_AGENT = "userAgent";
    public final static String CLIENT_OS = "client_os";
    public final static String MOBILE_BRAND = "mobile_brand";
    public final static String CLIENT_BROWSER = "client_browser";
    public final static String VERSION = "version";
    public final static String GROUPID = "groupId";

    ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<>();
    ReadWriteLock rwl = new ReentrantReadWriteLock();

    @Override
    public Map<String, String> LineToMap(String line, String dmac) {
        String dateStr;
        String day_id = "";

        Map<String, String> data = null;
        String rowkey = "";

        ZoneId zone = ZoneId.of("Asia/Shanghai");
        ZonedDateTime hostdate = Instant.now().atZone(zone);

        data = ConvertUtil.readJson2Map(line, dmac);
        if (data == null) {
            return null;
        }
        Date date = null;
        dateStr = data.get(TIME);
        try {
            date = new Timestamp(Long.parseLong(dateStr));
        } catch (NumberFormatException e) {
            Logger.getLogger(PV.class.getName()).log(Level.SEVERE, "hit　获取time失败:" + dateStr, e);
            return null;
        }

        if (!ConvertUtil.Validate(date))   return null; //过滤掉15天前的数据
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        day_id = sdf.format(date).substring(0, 10).replace("-", "");

        rowkey = getRowKey(data, date);

        data.put(DAY_ID, day_id);
        data.put(INDB_DATETIME, hostdate.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
        if (!data.containsKey(MAC)) {
            return null;
        }
        String mac = data.get(MAC);
        if (data.containsKey(USER_AGENT)) {
            data.putAll(ConvertUtil.getClientInfo(data.get(USER_AGENT)));
        }
        data.put(ROWKEY, rowkey);
        return data;
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddssHHmm");
        return sdf.format(date) + ConvertUtil.FormatMacString(obj.get(MAC)) + obj.get(DMAC)
                + obj.get(TIME).substring(obj.get(TIME).length() - 3);
    }
}
