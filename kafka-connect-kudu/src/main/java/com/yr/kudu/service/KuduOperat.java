package com.yr.kudu.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.yr.kudu.constant.SessionPool;
import com.yr.kudu.constant.TableTypeConstantMap;
import com.yr.kudu.utils.KuduUtil;
import com.yr.pojo.TableValues;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.*;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/17 4:19 下午
 */
@Slf4j
public class KuduOperat {
    private static BlockingQueue<KuduSession> sessionQueue = SessionPool.getSessions();
    public static void insert(TableValues tableValues) throws InterruptedException, KuduException {
        JSONObject json = JSONObject.parseObject(tableValues.getValus());
        String tableName = json.getString(tableValues.getTableName());
        KuduSession session = sessionQueue.take();
        KuduTable kuduTable = SessionPool.client.openTable(tableName);
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        Map<String,String> kuduTableType = TableTypeConstantMap.kuduTables.get(tableName);
        String[] arrays = (String[]) kuduTableType.keySet().toArray();
        for(int i = 0; i < arrays.length; i++){
            String key = arrays[i];
            String type = kuduTableType.get(key);
            KuduUtil.typeConversion(json, row, key, type);
        }
        session.apply(insert);
        judge(session);
        sessionQueue.add(session);
    }

    private static void judge(KuduSession session) {
        if (session.countPendingErrors() != 0) {
            log.info("errors inserting rows");
            RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            log.info("there were errors inserting rows to Kudu");
            log.info("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                log.info(errs[i]+"");
            }
            if (roStatus.isOverflowed()) {
                log.info("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
    }


}
