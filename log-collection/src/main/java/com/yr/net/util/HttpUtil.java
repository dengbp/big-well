package com.yr.net.util;

import com.yr.net.dto.HttpResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Description 向指定 URL 发送POST方法的请求
 * @return 远程资源的响应结果
 * @Author dengbp
 * @Date 14:02 2019-12-18
 **/
@Slf4j
public class HttpUtil {

    private static int TIMEOUT_MILLIS = 5000;
    /**
     * Description todo
     * @param url
     * @param operateType
     * @param param
     * @param response
     * @return int
     * @Author dengbp
     * @Date 18:14 2020-11-05
     **/
    public static int send(String url, String operateType, String param, HttpResponse response) {
        log.info("http请求,url[{}],操作类型[{}],参数[{}]",url,operateType,param);
        StringBuilder result = new StringBuilder();
        OutputStreamWriter out = null;
        InputStream is = null;
        HttpURLConnection conn = null;
        int response_code = 500;
        try {
            URL url1 = new URL(url);
            conn = (HttpURLConnection) url1.openConnection();
            /** 5秒 */
            conn.setConnectTimeout(TIMEOUT_MILLIS);
            conn.setReadTimeout(TIMEOUT_MILLIS);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod(operateType.toUpperCase());
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.connect();
            if (StringUtils.isNotBlank(param)) {
                out = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
                out.write(param);
                out.flush();
            }
            is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String str = "";
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(),"UTF-8");
                result.append(str);
            }
            response_code = conn.getResponseCode();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                conn.disconnect();
            }
        }
        response.setDesc(result.toString());
        log.info("连接器服务响应内容[{}]",response.getDesc());
        return response_code;
    }
}
