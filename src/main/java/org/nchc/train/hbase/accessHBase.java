package org.nchc.train.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 1403035 on 2014/8/11.
 */
public class accessHBase {
    private static Configuration conf = null;

    private static String tname = "t1";
    private static String cf1 = "cf1";
    private static String cf2 = "cf2";
    private static String ZK = "192.168.56.10";
    private static HTable htable;
    private static HBaseAdmin admin;
    public static void main(String[] args) throws IOException {

        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum",ZK);
        admin = new HBaseAdmin(conf);
        if (admin.tableExists(tname)) {
            System.out.println("table "+tname+" already exists!");
            delTable();
        } else {
            System.out.println("table "+tname+" not exist");
            createTable();
            htable = new HTable(conf,tname);
            testPut();
            testGet();
            testScan();
            htable.close();
        }
    }

    private static void createTable() throws IOException {
        System.out.println("create Table first");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tname);
        tableDescriptor.addFamily(new HColumnDescriptor(cf1));
        tableDescriptor.addFamily(new HColumnDescriptor(cf2));
        admin.createTable(tableDescriptor);
    }

    private static void delTable() throws IOException {
        System.out.println("Delete Table first");
        admin.disableTable(tname);
        admin.deleteTable(tname);
    }

    private static void testPut() throws IOException {
        List<Put> puts = new LinkedList<Put>();

        Put put = new Put(Bytes.toBytes("doe-john-m-12345"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("id"), Bytes.toBytes("12345"));
        puts.add(put);
        // NoSuchColumnFamily Exception
        // put.add(Bytes.toBytes("cf3"), Bytes.toBytes("email"), Bytes.toBytes("john.m.doe@gmail.com"));

        put = new Put(Bytes.toBytes("doe-john-m-23456"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("id"), Bytes.toBytes("23456"));
        puts.add(put);

        put = new Put(Bytes.toBytes("doe-john-m-34567"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("id"), Bytes.toBytes("34567"));
        puts.add(put);

        put = new Put(Bytes.toBytes("doe-john-f-12345"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mi"), Bytes.toBytes("F"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("id"), Bytes.toBytes("12345"));
        puts.add(put);

        put = new Put(Bytes.toBytes("doe-john-f-23456"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("mi"), Bytes.toBytes("F"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("surame"), Bytes.toBytes("Doe"));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("id"), Bytes.toBytes("23456"));
        puts.add(put);

        htable.put(puts);
    }

    private static void testGet() throws IOException {

        System.out.println("Get doe-john-m-12345");
        Get get = new Get(Bytes.toBytes("doe-john-m-12345"));
        Result result = htable.get(get);
        // One Row is one result
        printResult(result);
        System.out.println("===============");
    }

    private static void printResult(Result r){
        StringBuilder sb = new StringBuilder();
        sb.append("ROWKEY=").append(Bytes.toString(r.getRow())).append("\n").append("KV=[");
        for (KeyValue kv : r.raw()) {
            sb.append(Bytes.toString(kv.getFamily())).append(":").
                    append(Bytes.toString(kv.getQualifier())).append("=")
                    .append(Bytes.toString(kv.getValue())).append(",");

        }
        sb.append("]").append("\n");
        System.out.println(sb.toString());
    }

    private static void testScan() throws IOException {
        System.out.println("Scan full Table");
        fullTableScan();
        System.out.println("===============");
        System.out.println("scan all f");
        scanAllf();
        System.out.println("===============");
        System.out.println("scan [M1,M3)");
        scanM1M3();
        System.out.println("===============");
    }

    private static void fullTableScan() throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = htable.getScanner(scan);
        for(Result r :rs){
            printResult(r);
        }
    }

    private static void scanAllf() throws IOException {
        Scan scan = new Scan();
        byte[] start = Bytes.toBytes("doe-john-f");
        byte[] end = Bytes.toBytes("doe-john-g");
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = htable.getScanner(scan);
        for(Result r :rs){
            printResult(r);
        }
    }

    private static void scanM1M3() throws IOException {
        Scan scan = new Scan();
        byte[] start = Bytes.toBytes("doe-john-m-1");
        byte[] end = Bytes.toBytes("doe-john-m-3");
        scan.setStartRow(start);
        scan.setStopRow(end);
        ResultScanner rs  = htable.getScanner(scan);
        for(Result r :rs){
            printResult(r);
        }
    }
}
