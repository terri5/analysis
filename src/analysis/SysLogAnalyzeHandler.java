/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analysis;

import com.terri.model.Hit;
import com.terri.model.PV;
import com.terri.model.IHBaseModel;
import com.terri.model.NetMon;
import com.terri.util.HbaseUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author terri
 */
public class SysLogAnalyzeHandler {

    private static final Set<String> Hit_dmac = new HashSet<>();
    static IHBaseModel pvModel = new PV();
    static IHBaseModel pvModel2 = new PV();
    static IHBaseModel netmon = new NetMon();
    static IHBaseModel hitModel = new Hit();

    public static void SingleLogDirToDB(String filePath, BufferedReader reader) {

        String[] path = filePath.split("/");
        if (path.length != 3) {
            return;
        }
        String dmac = path[0];
        String blobName = path[1];
        String fileName = path[2];

        IHBaseModel handleModel = null;
        try {
            switch (fileName) {

                case IHBaseModel.LOCAL_PV_LOG:// 普列处理PV文件
                    handleModel = pvModel;
                    break;
                case IHBaseModel.PV_HTTP_LOG: // nginx access log 大巴日志 pv 日志
                    handleModel = pvModel;
                    break;
                case IHBaseModel.NGINX_PROXY_PV_LOG: //nginx 代理pv日志
                    handleModel = pvModel2;
                    break;
                case IHBaseModel.NET_MON_LOG: // net_mon_log 大巴日志 pv 日志
                    handleModel = netmon;
                    break;
                case IHBaseModel.HIT_LOG_FILE: //hit日志
                    handleModel = hitModel;
                    break;
            }
            if (handleModel != null) {
                handleLog2(dmac, reader, blobName, fileName, handleModel);
            }
            //     Trace.TraceError("--------->"+blobName+"  "+Hit_dmacs.Count+"");
        } catch (Exception ex) {
            Logger.getLogger(SysLogAnalyzeHandler.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                reader.close();
            } catch (IOException ex) {
                Logger.getLogger(SysLogAnalyzeHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public static void handleLog2(String dmac, BufferedReader reader, String blobName, String fileName, IHBaseModel model) {
        Set<String> ids;
        ids = new HashSet<>();
        if ("HIT".equals(model.GetColumnFamily()) && StringUtils.isNotEmpty(dmac)) {
            Hit_dmac.add(dmac.toLowerCase());
            //    MongoHelper.AddHitDmac(dmac.ToLower());

        }
        reader.lines().parallel().forEach((line) -> {
            Map<String, String> data = model.LineToMap(line, dmac);
            if (data == null) {
                return;
            }
            if (ids.add(data.get(IHBaseModel.ROWKEY))) {
                model.addMap(data);
            } else {
                return;
            }
            if ("PV".equals(model.GetColumnFamily())) {
                String url = data.get(PV.HTTP_URI);
                int pos = url.indexOf("hitID");
                if (pos > 0) {
                    //                                    Trace.TraceError("解析hitID");
                    Map hit = extractHitFromUrl(url.substring(pos), dmac, data);

                    if (hit != null) {
                        hitModel.addMap(hit);
                        checkPersist(hitModel);
                    }
                }
            }
           checkPersist(model);
        });
        ids.clear();

    }

    private static void checkPersist(IHBaseModel model) {
        if (model.shouldIntoHbase()) {
            ConcurrentLinkedQueue<Map<String, String>> tmpq = model.GetAndResetCacheQueueData();
            if(tmpq.isEmpty()) return ;
            System.out.println(LocalTime.now() + " finished " + model.GetColumnFamily() + ":" + tmpq.size());
            try {
                HbaseUtil.put(model, tmpq);
                System.out.println(LocalTime.now() + " write hbase finished " + model.GetColumnFamily() + " " + tmpq.size());
            } catch (Exception ex) {
                Logger.getLogger(SysLogAnalyzeHandler.class.getName()).log(Level.SEVERE, "写入hbase异常", ex);
            }

        }
    }

    public static void handleLog(String dmac, BufferedReader reader, String blobName, String fileName, IHBaseModel model) {

        String line = null;
        Set<String> ids = new HashSet<>();
        int filelines = 0;
        int bu_count = 0;
        int bu_count_repeat = 0;

        if ("HIT".equals(model.GetColumnFamily()) && StringUtils.isNotEmpty(dmac)) {
            Hit_dmac.add(dmac.toLowerCase());
            //    MongoHelper.AddHitDmac(dmac.ToLower());

        }

        try {
            while (null != (line = reader.readLine())) {
                filelines++;
                System.out.println(filelines+" "+line);
                Map<String, String> data = model.LineToMap(line, dmac);
                System.out.println(filelines+" finished");
                if (data != null) {
                    bu_count_repeat++;
                    if (ids.add(data.get(IHBaseModel.ROWKEY))) {
                        model.addMap(data);
                        bu_count++;

                        if ("PV".equals(model.GetColumnFamily()) && data.containsKey(PV.HTTP_URI)) {
                            String url = data.get(PV.HTTP_URI);
                            int pos = url.indexOf("hitID");
                            if (pos > 0) {
                                //                                    Trace.TraceError("解析hitID");
                                Map hit = extractHitFromUrl(url.substring(pos), dmac, data);
                                if (hit != null) {
                                    hitModel.addMap(hit);
                                    checkPersist(hitModel);

                                }
                            }

                        }

                    }

                }
                checkPersist(model);
            }

        } catch (IOException ex) {
            Logger.getLogger(SysLogAnalyzeHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(SysLogAnalyzeHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        ids.clear();
    }

    private static Map<String, String> extractHitFromUrl(String httpUri, String dmac, Map<String, String> data) {
        String[] str = httpUri.split("&");
        String json = "";
        Boolean notime = true;
        for (String tmp : str) {
            String[] item = tmp.split("=");
            if (item.length != 2) {
                continue;
            }
            if ("start_time".equals(item[0]) || "t".equals(item[0])) {
                json += ",\"time\":" + item[1];
                notime = false;
            } else {
                json += ",\"" + item[0] + "\":\"" + item[1] + "\"";
            }
        }
        if (notime) {
            return null;
        }

        if (!json.contains("\"mac\":\"")) {
            json += ",\"" + Hit.MAC + "\":\"" + data.get(PV.MAC) + "\"";
        }
        json += ",\"" + Hit.IP + "\":\"" + data.get(PV.IP) + "\"";
        json += ",\"" + Hit.USER_AGENT + "\":\"" + data.get(PV.CLIENT_AGENT) + "\"";
        json = "{" + json.substring(1) + "}";
      //  System.err.println("hit:" + json);
        return hitModel.LineToMap(json, dmac);
    }

}
