package com.yr.net.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @author dengbp
 * @ClassName HttpResponse
 * @Description TODO
 * @date 2020-11-06 14:06
 */
@Data
public class HttpResponse {


    private int code;
    private String desc;

    public HttpResponse() {
    }

    public HttpResponse(int code) {
        this.code = code;
    }

    public HttpResponse(String desc) {
        this.desc = desc;
    }

    public HttpResponse(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    @Override
    public  String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append("HttpResponse code=").append(code).append(",").append("desc=").append(desc);
        return sb.toString();
    }
}
