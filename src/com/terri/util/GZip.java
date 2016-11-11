/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.util;

import analysis.Analysis;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

/**
 *
 * @author terri
 */
public class GZip {
    public  static   Map<String,ByteArrayOutputStream>  unGzipFile(InputStream  pin ) throws IOException, ArchiveException {
        FileInputStream fis = null;
            ArchiveInputStream in = null;  
        BufferedInputStream bis = null;
           Map<String,ByteArrayOutputStream>  ots=null;
        String  outputDirectory="";
        try {
            GZIPInputStream gis = new GZIPInputStream(pin);
            in = new ArchiveStreamFactory().createArchiveInputStream("tar", gis);
            bis = new BufferedInputStream(in);
            TarArchiveEntry entry = (TarArchiveEntry) in.getNextEntry();
            ots=new HashMap<>();
            while (entry != null) {
                String name = entry.getName();
                String[] names = name.split("/");
                  String fileName = outputDirectory;  
                  if(names.length==1){
                               fileName=names[0];
                  }else{
                    for (String str : names) {
                        fileName = fileName + File.separator + str;
                    }         
                  }
             
           //         System.out.println("name="+fileName);
           
                if(!Analysis.FILE2HANDLE.contains(fileName)) {
                              entry = (TarArchiveEntry) in.getNextEntry();
                               continue;
                }

                   ByteArrayOutputStream  ot=new ByteArrayOutputStream();
                    BufferedOutputStream bos = new BufferedOutputStream(ot);
                    int b = -1;
                    while ((b = bis.read()) != -1) {
                        bos.write(b);
                    }
                    bos.flush();
                    bos.close();
              ots.put(fileName, ot);
                entry = (TarArchiveEntry) in.getNextEntry();
            }
            return  ots;
        }  finally {
                        if (null != bis) {
                            try {
                                bis.close();
                            } catch (IOException ex) {
                                Logger.getLogger(GZip.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                   }
    }
    
}
