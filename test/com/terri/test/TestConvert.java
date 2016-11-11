/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.test;

import com.terri.util.ConvertUtil;
import com.terri.util.MD5;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author terri
 */
public class TestConvert {

    String hit = "{\"time\":1476036422828,\"mac\":\"3c:ab:8e:b9:cd:7b\",\"ip\":\"192.168.17.81\",\"hitID\":\"null\",\"uID\":\"\",\"pageTime2\":\"13\",\"version\":\"T_2.4.9\",\"userAgent\":\"Mozilla/5.0 (Linux; Android 5.1; HUAWEI TIT-CL10 Build/HUAWEITIT-CL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.121 Mobile Safari/537.36\",\"browserID\":\"Mozilla/5.0 (Linux; Android 5.1; HUAWEI TIT-CL10 Build/HUAWEITIT-CL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/43.0.2357.121 Mobile Safari/537.36\",\"groupId\":\"2\"}";

    public TestConvert() {
    }
    @Test
    public void testRegex(){
     String str="Android444-AndroidPhone-8238-238-0-Statistics-wifi";
   //String str="Android444-AndroidPhone-8238-";
     String st3="(([a-zA-Z0-9_-])+(\\.)?)*(:\\d+)? ?(GET|POST|Get|Post|get|post)\\s{0,}\\S{2,}\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";
     String st2="((\\w|\\-)+(\\.)?){1,255}(:\\d+)? ";
      assertTrue(str.matches(st2));
    }
    @Test
    public void testHit() {
        Map<String, String> data = ConvertUtil.readJson2Map(hit, "1234");
        System.out.println(data);
        assertNotNull("hit 转换异常", data);
    }

    @Test
    public void TestParam() {
        //|(2\\d)
        // [(0[1-9])|(1[0-2])][(0[1-9])|((1|2)\\d)|(3[0-1])]
        String regex = "^20((1[6-9])|(2\\d))((0[1-9])|(1[0-2]))((0[1-9])|((1|2)\\d)|(3[0-1]))$";
        Assert.assertTrue("20161010".matches(regex));
    }

    @Test
    public void TestMd5() {
        String str = "{referer=, clientAgent=\"-\", dmac=gd200a15622222, ip=192.168.17.25, ROWKEY=20161010170312d4970b9b0915gd200a15622222XI6lntkh, DateTime_Point=2016-10-10 03:12:17, httpMethod=GET, day_id=20161010, mac=d4:97:0b:9b:09:15, httpVersion=HTTP/1.1, httpUri=www.wangfanwifi.com /bootstrap.html, INDB_DATETIME=20161013151217, httpHost=www.wangfanwifi.com}";
        System.out.println(MD5.stringMD5(str));
    }

    @Test
    public void testTimestamp() {
        Map<String, String> data = ConvertUtil.readJson2Map(hit, "1234");
        System.out.println(data);
        assertNotNull("hit 转换异常", data);
    }
      @Test
    public void testConveret(){
    String s="clientAgent => \"CaptiveNetworkSupport-306.3.1 wispr\"";
    String r= "(\")";
    System.out.println(s.replaceAll(r, ""));
    }
    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
}
