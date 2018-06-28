package com.kafka.core;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseConfig implements Serializable {
    private Configuration hconfig;

    public HbaseConfig() {
        this.hconfig = HBaseConfiguration.create();
        this.hconfig.set("hbase.cluster.distributed", ConfigUtil.getValByKey("hbase.cluster.distributed"));
        this.hconfig.set("hbase.zookeeper.quorum", ConfigUtil.getValByKey("hbase.zookeeper.quorum"));

        String hbaseSecurity = ConfigUtil.getValByKey("hbase.security.authentication");
        if ((hbaseSecurity != null) && (hbaseSecurity.equals("kerberos"))) {
            System.setProperty("java.security.krb5.conf", ConfigUtil.confPath + ConfigUtil.getValByKey("java.security.krb5.conf"));
            this.hconfig.set("hadoop.security.authentication", ConfigUtil.getValByKey("hadoop.security.authentication"));
            this.hconfig.set("hadoop.rpc.protection", ConfigUtil.getValByKey("hadoop.rpc.protection"));
            this.hconfig.set("hbase.security.authentication", ConfigUtil.getValByKey("hbase.security.authentication"));
            this.hconfig.set("hbase.rpc.protection", ConfigUtil.getValByKey("hbase.rpc.protection"));
            this.hconfig.set("hbase.master.kerberos.principal", ConfigUtil.getValByKey("hbase.master.kerberos.principal"));
            this.hconfig.set("hbase.regionserver.kerberos.principal", ConfigUtil.getValByKey("hbase.regionserver.kerberos.principal"));
            UserGroupInformation.setConfiguration(this.hconfig);
            try {
                UserGroupInformation user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(ConfigUtil.getValByKey("hbase.kerberos.user"), ConfigUtil.confPath + ConfigUtil.getValByKey("hbase.kerberos.user.keytab"));
                UserGroupInformation.setLoginUser(user);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Configuration getConfiguration() {
        return this.hconfig;
    }

    public Connection getHConnection() {
        Connection hconn = null;
        try {
//            ExecutorService executor = Executors.newFixedThreadPool(50);
            hconn = ConnectionFactory.createConnection(this.hconfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hconn;
    }

    public void closeHConnection(Connection hconn) {
        if ((hconn != null) && (!hconn.isClosed()))
            try {
                hconn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public boolean isClose(Connection hconn) {
        return hconn.isClosed();
    }
}