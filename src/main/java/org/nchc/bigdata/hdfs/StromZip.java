package org.nchc.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by 1403035 on 2014/10/14.
 */
public class StromZip {
    private static FileSystem fs;
    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://NN-agent:9000");
        fs = FileSystem.get(conf);
        //writeToHDFS("/D:/kafka_image/A.jpg", "/AAA.JPG");
        //readFromHDFS("/AAA.JPG","/D:/aaa.jpg");

        //Path pp = new Path("/lib/storm/0.9.2-incubating/storm.zip");
        Path pp = new Path("/zip/apach.zip");
        String ver = "0.9.2-incubating";
        System.out.println(getStormHomeInZip(fs ,pp, ver));
    }

    static String getStormHomeInZip(FileSystem fs, Path zip, String stormVersion) throws IOException, RuntimeException {
        FSDataInputStream fsInputStream = fs.open(zip);
        ZipInputStream zipInputStream = new ZipInputStream(fsInputStream);
        ZipEntry entry = zipInputStream.getNextEntry();
        while (entry != null) {
            String entryName = entry.getName();
            System.out.println(entryName);
            //if (entryName.matches("^storm(-" + stormVersion + ")?/")) {
            if (entryName.matches("^apache-storm(-" + stormVersion + ")?/") ||
                    entryName.matches("^storm(-" + stormVersion + ")?/")  ) {
                fsInputStream.close();

                return entryName.replace("/", "");
            }
            entry = zipInputStream.getNextEntry();
        }
        fsInputStream.close();
        throw new RuntimeException("Can not find storm home entry in storm zip file.");
    }


    private static void writeToHDFS(String localPath, String hdfsPath) throws IOException {
        FileInputStream fis = new FileInputStream(localPath);
        Path out = new Path(hdfsPath);
        OutputStream os = fs.create(out);
        org.apache.hadoop.io.IOUtils.copyBytes(fis, os, conf);
    }
}
