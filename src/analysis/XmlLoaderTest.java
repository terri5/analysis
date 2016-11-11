/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package analysis;

import java.util.Iterator;
import java.util.List;
import javax.naming.ConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;

/**
 *
 * @author terri
 */
public class XmlLoaderTest {
    
     public static void main(String[] args) throws ConfigurationException, org.apache.commons.configuration.ConfigurationException{  
       Configuration config;  

         config = new XMLConfiguration("cfg.xml");
       List<String> name = config.getList("handleFiles.file");  
       System.out.println("name:" + name);  
    }  


}
