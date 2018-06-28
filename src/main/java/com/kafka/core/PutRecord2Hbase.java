package com.kafka.core;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/9/8.
 */
public class PutRecord2Hbase {

    public static void main(String[] args) throws IOException {
        List<Put> listPut = new ArrayList<>();

        byte[] cf = Bytes.toBytes("data");

        Put p = new Put(Bytes.toBytes("64401.2013000211093560.1408015240000.UA"));
        p.addColumn(cf, Bytes.toBytes("10666256688989"), Bytes.toBytes("245.00"));
        listPut.add(p);

        Put p1 = new Put(Bytes.toBytes("rowkey_233333"));
        p1.addColumn(cf, Bytes.toBytes("q2"), Bytes.toBytes("245.7786"));
        listPut.add(p1);

        System.out.println("Save habse start.");
        HbaseConfig hbaseConfig = new HbaseConfig();
        Connection connection = hbaseConfig.getHConnection();
        Table table = connection.getTable(TableName.valueOf(ConfigUtil.getValByKey("hbase.table.name")));
        table.put(listPut);
        table.close();
        connection.close();
        System.out.println("Save habse successful.");
    }
}
