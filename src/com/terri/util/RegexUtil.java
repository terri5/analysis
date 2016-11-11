/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.util;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jdk.nashorn.internal.runtime.regexp.joni.Regex;

/**
 *
 * @author terri
 */
public class RegexUtil {

    private final static String IP_MAC_EX = "192\\.168(\\.(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))){2}(\\s|\\+[0-9a-zA-Z]{2}(:[0-9a-zA-Z]{2}){5})";
    static Pattern P_IP_MAC = Pattern.compile(IP_MAC_EX);

    //西文日期格式正则表达式  "\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2}\s{1}\+\d{4}"
    private final static String PV_DATE_EX = "\\d{2}/[a-zA-Z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s{1}\\+\\d{4}";
    static Pattern P_PV_DATE = Pattern.compile(PV_DATE_EX);

    public static final String DAY_ID_EX = "^20((1[6-9])|(2\\d))((0[1-9])|(1[0-2]))((0[1-9])|((1|2)\\d)|(3[0-1]))$";

    // URL
    public final static String URL_EXP = "((http|ftp|https)://)(([a-zA-Z0-9\\._-]+\\.[a-zA-Z]{2,6})|([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\\&%_\\./-~-]*)?";

    //Reuqest 
    public final static String REQUEST_EX = "(GET|POST|Get|Post|get|post)\\s{0,}\\S{2,}\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";

//http_host
    public final static String HTTP_HOST_EXP = "(([a-zA-Z0-9_-])+(\\.)?)*(:\\d+)?";

    public static Pattern P_HTTP_HOST = Pattern.compile(HTTP_HOST_EXP);

    private static final String HTTP_METHOD_EX = "(GET|POST|Get|Post|get|post)\\s{0,}";
    public final static Pattern P_HTTP_METHOD = Pattern.compile(HTTP_METHOD_EX);

    private static final String HTTP_PROTOCAL_EXP = "\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";
    public static Pattern P_HTTP_PROTOCAL = Pattern.compile(HTTP_PROTOCAL_EXP);

    private static final String regexHttpStatusAndBodySent = "\" \\d{3} \\d{1,} \"";
    private static final Pattern P_HTTP_STATUS_BODY_SENT = Pattern.compile(regexHttpStatusAndBodySent);

    // 引号之间
    public final static String QUOTES_EX = "\"(.*?)\"";

    public static String GetHttpStatusAndBodySent(String str) {
        Matcher m = P_HTTP_STATUS_BODY_SENT.matcher(str);
        if (m.find()) {
            return m.group().replace("\"", "").trim();
        }
        return null;
    }

    public static String getSingleIpString(String s) {
        Matcher m = P_IP_MAC.matcher(s);
        if (m.find()) {
            return m.group();
        }
        return null;
    }

    // 通过正则得到字符串中的日期值
    public static String getDateTimeString(String s) {

        Matcher m = P_PV_DATE.matcher(s);
        if (m.find()) {
            return m.group();
        }
        return null;
    }

    /// <summary>
    /// 得到引号中的数据
    /// </summary>
    /// <param name="regexStr"></param>
    /// <param name="s"></param>
    /// <returns></returns>
    public static List<String> GetStringsByRegex(String regexStr, String s) {
        List<String> list = new ArrayList<>();
        Pattern p_regexStr = Pattern.compile(regexStr);
        Matcher m = p_regexStr.matcher(s);
        while (m.find()) {
            list.add(m.group());
        }
        return list;
    }

}
