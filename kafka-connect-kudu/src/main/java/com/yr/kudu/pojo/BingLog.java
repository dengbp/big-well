package com.yr.kudu.pojo;

import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.HashMap;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/21 3:29 下午
 */
public class BingLog {

    public static final String DELETE = "d";
    public static final String UPDATE = "u";
    public static final String INSERT = "c";

    private String op;
    private CaseInsensitiveMap<String,String> before;
    private CaseInsensitiveMap<String,String> after;
    private HashMap<String,String> source;
    private Long ts_ms;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public CaseInsensitiveMap<String, String> getBefore() {
        return before;
    }

    public void setBefore(CaseInsensitiveMap<String, String> before) {
        this.before = before;
    }

    public CaseInsensitiveMap<String, String> getAfter() {
        return after;
    }

    public void setAfter(CaseInsensitiveMap<String, String> after) {
        this.after = after;
    }

    public HashMap<String, String> getSource() {
        return source;
    }

    public void setSource(HashMap<String, String> source) {
        this.source = source;
    }

    public Long getTs_ms() {
        return ts_ms;
    }

    public void setTs_ms(Long ts_ms) {
        this.ts_ms = ts_ms;
    }
}
