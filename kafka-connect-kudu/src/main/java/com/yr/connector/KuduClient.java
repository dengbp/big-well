package com.yr.connector;

import java.util.Map;

/**
 * @author dengbp
 * @ClassName KuduClient
 * @Description TODO
 * @date 2020-05-16 10:58
 */
public class KuduClient {

    private final Map<String, String> props;

    public KuduClient(Map<String, String> props) {
        this.props = props;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void close(){

    }
}
