package com.yr.net.collect;

import com.yr.net.dto.HttpResponse;
import com.yr.net.util.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author dengbp
 * @ClassName HttpCollect
 * @Description TODO
 * @date 2020-11-19 18:11
 */
@Slf4j
public class HttpCollect extends Collector{


    public HttpCollect(CollectType type) {
        super(type);
    }

    @Override
   public void collect() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(HttpCollect::execute);
    }


    private static void execute(){
        for (;;){
            HttpResponse httpResponse = new HttpResponse();
            HttpUtil.send("url","GET","{}",httpResponse);
            log.info("http result:{}",httpResponse.toString());
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
