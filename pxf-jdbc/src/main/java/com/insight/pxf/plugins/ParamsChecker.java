package com.insight.pxf.plugins;

/**
 * Created by jiadx on 2016/6/1.
 */
public class ParamsChecker {
    public static void assertTrue(String s, boolean b) {
        if(!b) throw new ParamsCheckException(s);
    }
}
class ParamsCheckException extends RuntimeException{
    ParamsCheckException(String msg) {
        super(msg);
    }
}
