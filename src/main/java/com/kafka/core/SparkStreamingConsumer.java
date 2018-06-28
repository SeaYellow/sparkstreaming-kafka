package com.kafka.core;

import kafka.serializer.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017/8/29.
 */
public class SparkStreamingConsumer {

    public static final int START_INDEX = 0;

    public static void main(String[] args) {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", ConfigUtil.getValByKey("kafka.bootstrap.servers"));

        Set<String> topics = new HashSet<>();
        SparkConf sc = new SparkConf();
        sc.setAppName("KafkaSparkStreamingHbase");
        sc.setMaster(ConfigUtil.getValByKey("spark.master"));
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(8000));
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jsc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                topics);

        JavaDStream<String> javaDStream = lines.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._2();
            }
        });

        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            public void call(JavaRDD<String> t) throws Exception {
                t.foreachPartition(new VoidFunction<Iterator<String>>() {

                    public void call(Iterator<String> t) throws Exception {
                        List<Put> listPut = new ArrayList<>();
                        while (t.hasNext()) {
                            java.sql.Connection conn = DatabaseUtil.getOracleConn();
                            String record = t.next();
                            // parse record
                            if (record == null || record.length() == 0) {
                                return;
                            }

                            String[] columns = record.split("\\|");
                            if (columns.length < 96) {
                                continue;
                            }
                            System.out.print("=========" + record + "=========");
                            String id = columns[0];
                            String phaseFlag = columns[2];

                            // 分表处理
                            int startIndex = -1;
                            String sql = "";
                            if (record.contains("E_MP_CURPHASE_CURVE")) { // 日测量点电流相位角曲线
                                phaseFlag = columns[5]; // 对应数据库的data_type
                                startIndex = 10; // 根据具体数据变
                                sql = "SELECT DISTINCT D.METER_ID AS ID,SUBSTR(d.ORG_NO,0,5) AS CITY_CODE,E.TG_ID AS TG_NO,TO_CHAR(A.GET_DATE,'DDMMYYY') AS GET_DATE FROM e_mp_power_curve1 A, P_MR_MPED D, C_MP E WHERE A.ID = ? AND A.ID = D.MPED_ID AND  E.MP_ID = D.MP_ID";
                                if ("1".equals(phaseFlag)) {
                                    phaseFlag = "A";
                                } else if ("2".equals(phaseFlag)) {
                                    phaseFlag = "AA";
                                } else if ("3".equals(phaseFlag)) {
                                    phaseFlag = "AB";
                                } else if ("4".equals(phaseFlag)) {
                                    phaseFlag = "AC";
                                } else if ("5".equals(phaseFlag)) {
                                    phaseFlag = "PAP";
                                } else if ("6".equals(phaseFlag)) {
                                    phaseFlag = "PRP";
                                } else if ("7".equals(phaseFlag)) {
                                    phaseFlag = "RAP";
                                } else if ("8".equals(phaseFlag)) {
                                    phaseFlag = "RRP";
                                }
                            } else if (record.contains("e_mp_vol_curve1")) { // 电压表
                                phaseFlag = columns[2];
                                startIndex = 7; // 根据具体数据变
                                sql = "SELECT DISTINCT D.METER_ID AS ID,SUBSTR(d.ORG_NO,0,5) AS CITY_CODE,E.TG_ID AS TG_NO,TO_CHAR(A.GET_DATE,'DDMMYYY') AS GET_DATE FROM e_mp_vol_curve1 A, P_MR_MPED D, C_MP E WHERE A.ID = ? AND A.ID = D.MPED_ID  AND E.MP_ID = D.MP_ID";
                                // A.PHASE_FLAG 里面的1代表UA,2代表UB,3代表UC  0代表U0
                                if ("1".equals(phaseFlag)) {
                                    phaseFlag = "UA";
                                } else if ("2".equals(phaseFlag)) {
                                    phaseFlag = "UB";
                                } else if ("3".equals(phaseFlag)) {
                                    phaseFlag = "UC";
                                } else if ("0".equals(phaseFlag)) {
                                    phaseFlag = "U0";
                                }
                            } else if (record.contains("e_mp_cur_curve1")) { // 电流表
                                sql = "SELECT DISTINCT D.METER_ID AS ID,SUBSTR(d.ORG_NO,0,5) AS CITY_CODE,E.TG_ID AS TG_NO,TO_CHAR(A.GET_DATE,'DDMMYYY') AS GET_DATE FROM e_mp_cur_curve1 A, P_MR_MPED D, C_MP E WHERE A.ID = ? AND A.ID = D.MPED_ID AND  E.MP_ID = D.MP_ID";
                            }

                            String meterId = null;
                            String cityCode = null;
                            String tgNo = null;
                            String day = null;
                            try (PreparedStatement pre = conn.prepareStatement(sql)) {
                                pre.setInt(1, Integer.parseInt(id));
                                ResultSet result = pre.executeQuery();
                                while (result.next()) {
                                    meterId = result.getString(1);
                                    cityCode = result.getString(2);
                                    tgNo = result.getString(3);
                                    day = result.getString(4);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            // close connection
                            DatabaseUtil.colseConn(conn);

                            // put into hbase
                            int addTime = 15;//rowkey 时间间隔
                            byte[] cf = Bytes.toBytes(ConfigUtil.getValByKey("hbase.cf"));
                            // 生产主键和值
                            for (int i = 0; i < 96; i++) {
                                int time = i * addTime + addTime;
                                String getDate = day + Utils.getHour(time) + "00";
                                String rowKey = cityCode + "." + tgNo + "." + getDate + "." + phaseFlag;
                                String dataValue = columns[i + startIndex];
                                System.out.println("rowKey : " + rowKey + " dataValue : " + dataValue);
                                Put p = new Put(Bytes.toBytes(rowKey));
                                p.addColumn(cf, Bytes.toBytes(meterId), Bytes.toBytes(dataValue));
                                listPut.add(p);
                            }
                        }

                        if (listPut.size() == 0) {
                            return;
                        }
                        Connection hconn = null;
                        try {
                            System.out.println("Save Hbase start.");
                            hconn = ConnectionFactory.createConnection(getHConfig());
                            SimpleDateFormat sdf = new SimpleDateFormat("YYYYMM");
                            String ym = sdf.format(new Date());
                            String tableName = ConfigUtil.getValByKey("hbase.table.name") + "_" + ym;
                            createHbaseTable(hconn, tableName);
                            Table table = hconn.getTable(TableName.valueOf(tableName));
                            table.put(listPut);
                            table.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                            StringWriter stringWriter = new StringWriter();
                            PrintWriter printWriter = new PrintWriter(stringWriter);
                            e.printStackTrace(printWriter);
                            StringBuffer error = stringWriter.getBuffer();
                            System.out.println(error);
                        } finally {
                            if (hconn != null && !hconn.isClosed()) {
                                hconn.close();
                            }
                        }
                        System.out.println("Save Hbase end.");
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
    }


    public static Configuration getHConfig() {
        Configuration hconfig = HBaseConfiguration.create();
        hconfig = HBaseConfiguration.create();
        hconfig.set("hbase.cluster.distributed", ConfigUtil.getValByKey("hbase.cluster.distributed"));
        hconfig.set("hbase.zookeeper.quorum", ConfigUtil.getValByKey("hbase.zookeeper.quorum"));

        String hbaseSecurity = ConfigUtil.getValByKey("hbase.security.authentication");
        if ((hbaseSecurity != null) && (hbaseSecurity.equals("kerberos"))) {
            System.setProperty("java.security.krb5.conf", ConfigUtil.confPath + ConfigUtil.getValByKey("java.security.krb5.conf"));
            hconfig.set("hadoop.security.authentication", ConfigUtil.getValByKey("hadoop.security.authentication"));
            hconfig.set("hadoop.rpc.protection", ConfigUtil.getValByKey("hadoop.rpc.protection"));
            hconfig.set("hbase.security.authentication", ConfigUtil.getValByKey("hbase.security.authentication"));
            hconfig.set("hbase.rpc.protection", ConfigUtil.getValByKey("hbase.rpc.protection"));
            hconfig.set("hbase.master.kerberos.principal", ConfigUtil.getValByKey("hbase.master.kerberos.principal"));
            hconfig.set("hbase.regionserver.kerberos.principal", ConfigUtil.getValByKey("hbase.regionserver.kerberos.principal"));
            UserGroupInformation.setConfiguration(hconfig);
            try {
                UserGroupInformation user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(ConfigUtil.getValByKey("hbase.kerberos.user"), ConfigUtil.confPath + ConfigUtil.getValByKey("hbase.kerberos.user.keytab"));
                UserGroupInformation.setLoginUser(user);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return hconfig;
    }

    public static void createHbaseTable(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName hTableName = TableName.valueOf(tableName);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("data");
        hTableDescriptor.addFamily(hColumnDescriptor);
        if (!admin.tableExists(hTableName)) {
            admin.createTable(hTableDescriptor);
            System.out.println("Create hbase table " + tableName + "successful.");
        }
    }
}
