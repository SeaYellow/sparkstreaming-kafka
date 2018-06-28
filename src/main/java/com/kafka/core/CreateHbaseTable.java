package com.kafka.core;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by Administrator on 2017/9/8.
 */
public class CreateHbaseTable {
    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 1) {
            System.out.println("Args is wrong.");
            return;
        }
        String opType = args[0];
        HbaseConfig hbaseConfig = new HbaseConfig();
        Connection connection = hbaseConfig.getHConnection();
        Admin admin = connection.getAdmin();
        String hbaseTableName = ConfigUtil.getValByKey("hbase.table.name");
        System.out.println("Hbase table name : " + hbaseTableName);
        TableName tableName = TableName.valueOf(hbaseTableName);

        if ("d".equalsIgnoreCase(opType)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } else if ("q".equalsIgnoreCase(opType)) {
            if (admin.tableExists(tableName)) {
                System.out.println("Hbase talbe is exists!");
                Table table = connection.getTable(tableName);
                Scan scan = new Scan();
                ResultScanner resultScanner = table.getScanner(scan);
                for (Result result : resultScanner) {
                    showCell(result);
                }
                table.close();
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("data");
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
                System.out.println("Create Hbase talbe successful!");
            }
        }

        admin.close();
        connection.close();
    }

    //格式化输出
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp: " + cell.getTimestamp() + " ");
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
        }
    }

}
