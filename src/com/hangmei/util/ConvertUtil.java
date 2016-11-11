/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hangmei.util;

import analysis.Analysis;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hangmei.model.PV;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.time.DateUtils;
import sun.misc.BASE64Encoder;

/**
 *
 * @author terri
 */
public class ConvertUtil {

    private final static Set<String> mobile_os_dict = new HashSet<>();

    private final static Set<String> mobile_browser_dict = new HashSet<>();

    private final static Set<String> mobile_brand_dict = new HashSet<>();

    private static Pattern p_os = null;
    private static Pattern p_brand = null;
    private static Pattern p_browser = null;
    private static Date start_datetime;

    static {//初始化字典
        SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
        Date date = null;
        try {
            start_datetime = f.parse(Analysis.DAY_ID);
        } catch (ParseException ex) {
            Logger.getLogger(PV.class.getName()).log(Level.SEVERE, "日期初始化异常　day_id:" + Analysis.DAY_ID, ex);
        }
        final StringBuilder os_str_regex = new StringBuilder();
        final StringBuilder brand_str_regex = new StringBuilder();
        final StringBuilder browser_str_regex = new StringBuilder();
        mobile_os_dict.add("Android");
        mobile_os_dict.add("iPhone OS");
        mobile_os_dict.add("Windows NT");
        mobile_os_dict.forEach((String os) -> {
            os_str_regex.append("(").append(os).append(")").append("|");
        });

        os_str_regex.deleteCharAt(os_str_regex.length() - 1);
        p_os = Pattern.compile(os_str_regex.toString());

        mobile_browser_dict.add("QQBrowser");
        mobile_browser_dict.add("Firefox");
        mobile_browser_dict.add("Chrome");
        mobile_browser_dict.add("Safari");
        mobile_browser_dict.add("Opera");
        mobile_browser_dict.add("UCBrowser");
        mobile_browser_dict.add("360Browser");

        mobile_browser_dict.forEach((String browser) -> {
            browser_str_regex.append("(").append(browser).append(")").append("|");
        });

        browser_str_regex.deleteCharAt(browser_str_regex.length() - 1);
        p_browser = Pattern.compile(browser_str_regex.toString());

        mobile_brand_dict.add("iPhone");
        mobile_brand_dict.add("HUAWEI");
        mobile_brand_dict.add("MIUI");
        mobile_brand_dict.add("SAMSUNG");
        mobile_brand_dict.add("OPPO");
        mobile_brand_dict.add("MEIZU");
        mobile_brand_dict.add("VIVO");
        mobile_brand_dict.add("Coolpad");
        mobile_brand_dict.add("HTC");
        mobile_brand_dict.add("ZTE");
        mobile_brand_dict.add("Lenovo");
        mobile_brand_dict.add("sony");
        mobile_brand_dict.forEach((String brand) -> {
            brand_str_regex.append("(").append(brand).append(")").append("|");
        });
        brand_str_regex.deleteCharAt(brand_str_regex.length() - 1);
        p_brand = Pattern.compile(brand_str_regex.toString());

    }

    /**
     *
     * @param ca clientAgent
     * @return
     */
    public static Map<String, String> getClientInfo(String ca) {
        Map<String, String> t = new HashMap<>();
        Matcher m = p_os.matcher(ca);
        if (!m.find()) {
            return t;
        }
        t.put(PV.CLIENT_OS, m.group());
        m = p_brand.matcher(ca);
        if (m.find()) {
            t.put(PV.MOBILE_BRAND, m.group());
        }
        m = p_browser.matcher(ca);
        if (m.find()) {
            t.put(PV.CLIENT_BROWSER, m.group());
        }
        return t;
    }

    public static boolean Validate(Date date) {
        return Math.abs(daysBetween(start_datetime, date)) <= 15;
    }
    

    /**
     * 计算两个日期之间相差的天数
     *
     * @param smdate 较小的时间
     * @param bdate 较大的时间
     * @return 相差天数
     * @throws ParseException
     */
    private static int daysBetween(Date smdate, Date bdate) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            smdate = sdf.parse(sdf.format(smdate));
            bdate = sdf.parse(sdf.format(bdate));
            Calendar cal = Calendar.getInstance();
            cal.setTime(smdate);
            long time1 = cal.getTimeInMillis();
            cal.setTime(bdate);
            long time2 = cal.getTimeInMillis();
            long between_days = (time2 - time1) / (1000 * 3600 * 24);
            return Integer.parseInt(String.valueOf(between_days));
        } catch (ParseException ex) {
            Logger.getLogger(ConvertUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 999;
    }

    /**
     * 字符串的日期格式的计算
     */
    public static int daysBetween(String smdate, String bdate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(smdate));
        long time1 = cal.getTimeInMillis();
        cal.setTime(sdf.parse(bdate));
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    public static String FormatMacString(String mac) {
        return mac.replace("-", "").replace("_", "").replace(":", "").replace("\n", "").replace("\r", "");
    }
    
    
    //从Blobname中获取日期
    public static String getPackageTimeStr(String blobName) {
        String tmp = blobName.replace("-", "").replace(":", "");
        //从Blobname中获取日期    
        return  tmp.substring(tmp.length()-19, tmp.length()-7);
    }
    public static Date getPackageTime(String blobName){
              String dateStr=getPackageTimeStr(blobName);
              SimpleDateFormat sdf=new SimpleDateFormat("yyMMddHHmmss");
        try {
            return sdf.parse(dateStr);
        } catch (ParseException ex) {
            Logger.getLogger(ConvertUtil.class.getName()).log(Level.SEVERE,"从包名获取日期异常："+blobName, ex);
            return null;
        }
    }

    public static final String EncodeByMd5(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        return MD5.stringMD5(str);
        /*
        // 确定计算方法  
        MessageDigest md5 = MessageDigest.getInstance("MD5");  
        BASE64Encoder base64en = new BASE64Encoder();  
        // 加密后的字符串  
        String newstr = base64en.encode(md5.digest(str.getBytes("utf-8")));  
        return newstr;  
         */
    }

    public static Map<String, String> readJson2Map(String line, String dmac) {
        Map<String, String> map = new HashMap<>();
        try {

            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> maps = objectMapper.readValue(line, Map.class);
            maps.entrySet().forEach((entry) -> {
                map.put(entry.getKey(), entry.getValue().toString());
            });
            return map;
        } catch (JsonParseException e) {
            Logger.getLogger(ConvertUtil.class.getName()).log(Level.SEVERE, null, e);
        } catch (JsonMappingException e) {
            Logger.getLogger(ConvertUtil.class.getName()).log(Level.SEVERE, null, e);
        } catch (IOException e) {
            Logger.getLogger(ConvertUtil.class.getName()).log(Level.SEVERE, null, e);
        }
        return null;
    }

}
