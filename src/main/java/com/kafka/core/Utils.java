package com.kafka.core;

/**
 * Created by Administrator on 2017/9/7.
 */
public class Utils {
    public static String getHour(int minutes) {
        int h = minutes / 60;
        int m = minutes - h * 60;
        String hour;
        String minute;
        if (String.valueOf(h).length() == 1) {
            hour = "0" + h;
        } else {
            hour = String.valueOf(h);
        }
        if (String.valueOf(m).length() == 1) {
            minute = "0" + m;
        } else {
            minute = String.valueOf(m);
        }
        return hour + minute;
    }


    public static void main(String[] args) {
        System.out.println(Utils.getHour(1440));
    }
}
