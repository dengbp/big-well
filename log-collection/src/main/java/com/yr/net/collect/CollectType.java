package com.yr.net.collect;

/**
 * @author dengbp
 * @ClassName CollectType
 * @Description TODO
 * @date 2020-11-19 18:21
 */
public enum CollectType {

    /** http采集 */
    HTTP_COLLECT("01",HttpCollect.class,"http采集");

    /** 类型编码 */
    private String code;

    /** 采集类型 */
    private Class tClass;

    /** 类型名称 */
    private String name;




    CollectType(String code, Class tClass,String name) {
        this.code = code;
        this.tClass = tClass;
        this.name = name;
    }

    public String getCode() {
        return code;
    }


    public Class getTClass() {
        return tClass;
    }

    public String getName() {
        return name;
    }}
