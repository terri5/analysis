/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analysis;

import com.hangmei.util.ConvertUtil;
import com.hangmei.util.GZip;
import com.hangmei.util.RegexUtil;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.BufferedReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author terri
 */
public class Analysis {

    static String storageConnectionString;
    public static JedisPool jedisPool;
    public static HashMap<String, HashSet<String>> fac_fname = new HashMap<String, HashSet<String>>();

    public static String DAY_ID;
    private static int span = 1;

    public static Set<String> FILE2HANDLE = new HashSet<>();
    static Configuration config;

    static {
        fac_fname.put("中兴高达", new HashSet<String>());
        fac_fname.put("傲天", new HashSet<String>());
        fac_fname.put("锐捷58", new HashSet<String>());
        fac_fname.put("锐捷", new HashSet<String>());
        fac_fname.put("宏电", new HashSet<String>());
        fac_fname.put("中太", new HashSet<String>());
        fac_fname.put("傲天证", new HashSet<String>());
        loadCfg();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        getArgs(args);
        Date d = new Date();
        GregorianCalendar gc = new GregorianCalendar();
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
        try {
            gc.setTime(sf.parse(DAY_ID));
        } catch (ParseException ex) {
            Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        for (int i = 0; i < span; i++) {
            handlleContainer(DAY_ID);
            gc.add(GregorianCalendar.DAY_OF_YEAR, -1);
        }

    }

    private static void getArgs(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("必须输入日期参数　day_id");
        } else {
            if (!args[0].matches(RegexUtil.DAY_ID_EX)) {
                throw new IllegalArgumentException("day_id　参数格式不符合要求" + args[0]);
            }
            DAY_ID = args[0];
            if (args.length == 2) {
                if (!args[1].matches("^-?\\d+$")) {
                    throw new IllegalArgumentException("span 要求是整数");
                }
                span = Integer.parseInt(args[1]);
            }
            if (args.length > 2) {
                throw new IllegalArgumentException("最多包含两个参数");
            }
        }
        System.out.println("init paramteres day_id:" + DAY_ID + " span:" + span);
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException ex) {
            Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void handlleContainer(String day_id) {

        try {
            System.out.println("blob url: " + storageConnectionString);
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            //       Create the blob client.
            System.out.println("account " + storageAccount.getBlobStorageUri());
            CloudBlobClient blobClient;
            blobClient = storageAccount.createCloudBlobClient();
            //         System.out.println("blobclient "+blobClient.getServiceStats());
            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.getContainerReference(day_id);
            System.out.println("container " + container.getUri());
            // Create the container if it does not exist.
            boolean exist = container.exists();
            System.out.println("exist:" + exist);
            // Loop over blobs within the container and output the URI to each of them.

            for (ListBlobItem blobItem : container.listBlobs()) {

                if (blobItem instanceof CloudBlobDirectory) {
                    List<CloudBlob> blobs = new ArrayList<>();
                    Collections.synchronizedCollection(blobs);
                    // Download the item and save it to a file with the same name.
                    CloudBlobDirectory blobDir = (CloudBlobDirectory) blobItem;
                    System.out.println(LocalTime.now() + " handle dir..." + blobDir.getPrefix());
                    //DownloadFiles(blobDir);
                    String dir = blobDir.getUri().getPath();
                    blobDir.listBlobs().forEach((ListBlobItem blobItem2) -> { //迭代处理每个blob文件

                        if (blobItem2 instanceof CloudBlob) {

                            // Download the item and save it to a file with the same name.
                            CloudBlob blob = (CloudBlob) blobItem2;
                            String blobPrefix = blob.getName().substring(0, 10).toLowerCase();
                            blobs.add(blob);

                        }
                    });
                   // blobs.forEach((CloudBlob blob) -> {
                   blobs.parallelStream().forEach((CloudBlob blob) -> {
                        DownloadFile(dir, blob);
                    });
                }

            }
        } catch (StorageException | URISyntaxException | InvalidKeyException ex) {
            Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void DownloadFile(String dir, CloudBlob blob) {
        String blobName = blob.getName().substring(blob.getName().indexOf("/") + 1);
        try {
            if (filter(dir, blobName)) {
                return;
            }

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
          //  System.out.println(LocalTime.now() + " download..." + blob.getName());
            blob.download(bout);

            InputStream zin = new ByteArrayInputStream(bout.toByteArray());

            Map<String, ByteArrayOutputStream> ots = null;
            try {
                ots = GZip.unGzipFile(zin);
            } catch (IOException | ArchiveException ex) {
                Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
            }
           //  System.out.println(LocalTime.now()+" 解压完成..."+blob.getName());
            if (ots == null || ots.isEmpty()) {
                return;
            }
            /**
             *
             * if(blobPrefix.startsWith("gd200")){
             * fac_fname.get("中兴高达").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("001f64")||blobPrefix.startsWith("4c48da")){
             * fac_fname.get("傲天").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("58696c")){
             * fac_fname.get("锐捷58").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("c20140")){
             * fac_fname.get("宏电").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("0008d2")){
             * fac_fname.get("中太").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("f0deb9")){
             * fac_fname.get("傲天证").addAll(ots.keySet()); }else
             * if(blobPrefix.startsWith("00f806")||blobPrefix.startsWith("hmap")){
             * fac_fname.get("锐捷").addAll(ots.keySet()); }
             */

            ots.entrySet().parallelStream().filter(e -> e.getValue().size() > 0).forEach((Entry<String, ByteArrayOutputStream> e) -> {
                //  ots.entrySet().stream().filter(e -> e.getValue().size() > 0).forEach((Entry<String, ByteArrayOutputStream> e) -> {
                try (InputStream in = new ByteArrayInputStream(e.getValue().toByteArray())) {
                    InputStreamReader isr = new InputStreamReader(in);
                    try (BufferedReader bufr = new BufferedReader(isr)) {
                        String fileName = blob.getName() + "/" + e.getKey();
                        //   System.out.println(LocalTime.now() + " 开始解析 " + e.getKey());
                        SysLogAnalyzeHandler.SingleLogDirToDB(fileName, bufr);
                        //    System.out.println(LocalTime.now() + " 解析完 " + e.getKey());
                    }
                } catch (IOException ex) {
                    Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
           // System.out.println(LocalTime.now() + " 解析完成 " + blob.getName());

        } catch (StorageException ex) {
            Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static boolean filter(String path, String blobName) {
        Date d = ConvertUtil.getPackageTime(blobName);
        if (d == null || !ConvertUtil.Validate(d)) {
            return true;
        }
        long added;
        try (Jedis jedis = jedisPool.getResource()) {
            added = jedis.sadd("off:blob" + path.substring(0, path.length() - 1).replace("/", ":"), blobName);
        }
        return added == 0;
    }

    private static void loadCfg() {
        try {
            config = new XMLConfiguration("cfg.xml");
            List<String> name = config.getList("handleFiles.file");
            FILE2HANDLE.addAll(name);
            storageConnectionString = config.getString("storageConnectionString");
            String redis_host = config.getString("redis.host");
            String redis_auth = config.getString("redis.auth");
            int redis_port = config.getInt("redis.port");
            int timeout = config.getInt("redis.timeout");
            int redis_db = config.getInt("redis.db");
            int max_cnn = config.getInt("redis.max_cnn");
            System.out.println("auth=" + redis_auth + " bool=" + StringUtils.isEmpty(redis_auth));
            if (StringUtils.isEmpty(redis_auth)) {
                GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
                cfg.setMaxTotal(max_cnn);
                jedisPool = new JedisPool(cfg, redis_host);
            } else {
                GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
                cfg.setMaxTotal(max_cnn);
                jedisPool = new JedisPool(cfg, redis_host, redis_port, timeout, redis_auth, redis_db);
            }
            System.out.println("cfg load complete");
        } catch (ConfigurationException ex) {
            Logger.getLogger(Analysis.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
