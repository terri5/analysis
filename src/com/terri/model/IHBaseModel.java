/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.model;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author terri
 */
public interface IHBaseModel {

    String LOCAL_PV_LOG = "localhost.log";
    String PV_HTTP_LOG = "pv_http.log";
    String NGINX_PROXY_PV_LOG = "nginxproxy.log";
    String NET_MON_LOG = "netmon.log";
    String HIT_LOG_FILE = "hit.log";
    String ROWKEY = "ROWKEY";

    public String GetColumnFamily();

    public Map<String, String> LineToMap(String line, String dmac);

    public void addMap(Map<String, String> data);

    public boolean shouldIntoHbase();

    public ConcurrentLinkedQueue<Map<String, String>> GetAndResetCacheQueueData();

    public String getRowKey(Map<String, String> m, Date d);

    public String getTableName();

}
