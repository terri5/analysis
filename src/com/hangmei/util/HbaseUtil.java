/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hangmei.util;

import com.hangmei.model.IHBaseModel;
import static com.hangmei.util.HbaseUtil.put;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author terri
 */
public class HbaseUtil {

    static final Configuration cfg = HBaseConfiguration.create();
    static Connection connection;

    static {
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.zookeeper.quorum", "172.16.2.41,172.16.2.42,172.16.2.43");
       // cfg.set("hbase.master", "airmediahbasev3.azurehdinsight.cn:60000");
        try {
            connection = ConnectionFactory.createConnection(cfg);
        } catch (IOException ex) {
            Logger.getLogger(HbaseUtil.class.getName()).log(Level.SEVERE, "初始化configuration异常", ex);
        }
    }

    public static void put(String tablename, List<Put> list) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tablename));
        table.put(list);
        System.out.println("put "+list.size());
    }

    public static void put(IHBaseModel model, Collection<Map<String, String>> list) throws Exception {
        if(list.isEmpty()) return ;
        List<Put> rst = Collections.synchronizedList(new ArrayList<>());
        list.parallelStream().forEach((Map<String, String> map) -> {
            String row = map.get(IHBaseModel.ROWKEY);
            Put put = new Put(Bytes.toBytes(row));
            map.entrySet().stream().forEach((entry) -> {
                put.addColumn(Bytes.toBytes(model.GetColumnFamily()), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            });
            rst.add(put);
        });
        put(model.getTableName(), rst);
    }
}
