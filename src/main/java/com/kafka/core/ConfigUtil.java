package com.kafka.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Administrator on 2017/9/7.
 */
public class ConfigUtil {
    public static Properties properties = new Properties();
    public static String confPath = "";

    static {
        String userDir = System.getProperty("user.dir");
        System.out.println("Config dir is : " + userDir);
        String parent = new File(userDir).getParent();
        confPath = parent + File.separator + "config" + File.separator;
        try {
            properties.load(new FileInputStream(new File(confPath + "config.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getValByKey(String key) {
        return properties.getProperty(key);
    }

}
