package com.insight.pxf.plugins.jdbc.utils;

import org.apache.hawq.pxf.api.utilities.InputData;

import java.sql.Connection;

/**
 * Created by jiadx on 2016/5/13.
 */
public class JdbcUtil {

    public static byte[] mergeBytes(byte[] b1, byte[] b2) {
        byte[] newb = new byte[b1.length + b2.length];
        int i = 0;
        for (byte b : b1) newb[i++] = b;
        for (byte b : b2) newb[i++] = b;
        return newb;
    }

    public static byte[][] splitBytes(byte[] bytes, int n) {
        int len = bytes.length / n;
        byte[][] newb = new byte[len][];
        int j = 0;
        for (int i = 0; i < len; i++) {
            newb[i] = new byte[n];
            for (int k = 0; k < n; k++) newb[i][k] = bytes[j++];
        }
        return newb;
    }

    public static byte[] getBytes(long value) {
        byte[] b = new byte[8];
        b[0] = (byte) ((value >> 56) & 0xFF);
        b[1] = (byte) ((value >> 48) & 0xFF);
        b[2] = (byte) ((value >> 40) & 0xFF);
        b[3] = (byte) ((value >> 32) & 0xFF);
        b[4] = (byte) ((value >> 24) & 0xFF);
        b[5] = (byte) ((value >> 16) & 0xFF);
        b[6] = (byte) ((value >> 8) & 0xFF);
        b[7] = (byte) ((value >> 0) & 0xFF);
        return b;
    }

    public static byte[] getBytes(int value) {
        byte[] b = new byte[4];
        b[0] = (byte) ((value >> 24) & 0xFF);
        b[1] = (byte) ((value >> 16) & 0xFF);
        b[2] = (byte) ((value >> 8) & 0xFF);
        b[3] = (byte) ((value >> 0) & 0xFF);
        return b;
    }

    public static int toInt(byte[] b) {
        return (((((int) b[3]) & 0xFF) << 32) +
                ((((int) b[2]) & 0xFF) << 40) +
                ((((int) b[1]) & 0xFF) << 48) +
                ((((int) b[0]) & 0xFF) << 56));
    }

    public static long toLong(byte[] b) {
        return ((((long) b[7]) & 0xFF) +
                ((((long) b[6]) & 0xFF) << 8) +
                ((((long) b[5]) & 0xFF) << 16) +
                ((((long) b[4]) & 0xFF) << 24) +
                ((((long) b[3]) & 0xFF) << 32) +
                ((((long) b[2]) & 0xFF) << 40) +
                ((((long) b[1]) & 0xFF) << 48) +
                ((((long) b[0]) & 0xFF) << 56));
    }
}
