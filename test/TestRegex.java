
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author terri
 */
public class TestRegex {
                 public final static  String  RegexUrl = "((http|ftp|https)://)(([a-zA-Z0-9\\._-]+\\.[a-zA-Z]{2,6})|([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\\&%_\\./-~-]*)?";
                     //Reuqest 
         public final static  String RexgexRequest = "(GET|POST|Get|Post|get|post)\\s{0,}\\S{2,}\\s{0,}(HTTP|http|Http)/\\d{0,}.\\d{0,}";
             
                 static     Pattern  p=Pattern.compile(RexgexRequest);  
      public static void main(String args[]){
          Matcher m=p.matcher("192.168.17.87+68:3e:34:9a:19:08 - - [10/Sep/2016:23:05:51 +0800] \"captive.apple.com GET / HTTP/1.1\" 200 191 \"-\" \"Dalvik/2.1.0 (Linux; U; Android 5.1; m1 metal Build/LMY47I)\" -");
          while(m.find())
                          System.out.println(m.group());
      
      
      
      
      
      
      
      
      }
}
