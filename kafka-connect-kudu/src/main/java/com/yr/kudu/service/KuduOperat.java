package com.yr.kudu.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.yr.kudu.constant.SessionPool;
import com.yr.kudu.constant.TableTypeConstantMap;
import com.yr.kudu.utils.KuduUtil;
import org.apache.kudu.client.*;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * @author baiyang
 * @version 1.0
 * @date 2020/5/17 4:19 下午
 */
public class KuduOperat {
    private static BlockingQueue<KuduSession> sessionQueue = SessionPool.getSessions();
    public static void insert(String source) throws InterruptedException, KuduException {
        JSONObject json = JSON.parseObject(source);
        String tableName = json.getString("tableName");
        KuduSession session = sessionQueue.take();
        KuduTable kuduTable = SessionPool.client.openTable(tableName);
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        Map<String,String> kuduTableType = TableTypeConstantMap.billTables.get(tableName);
        String[] arrays = (String[]) kuduTableType.keySet().toArray();
        for(int i = 0; i < arrays.length; i++){
            String key = arrays[i];
            String type = kuduTableType.get(key);
            KuduUtil.typeConversion(json, row, key, type);
        }
        session.apply(insert);

        judge(session);
    }

    private static void judge(KuduSession session) {
        if (session.countPendingErrors() != 0) {
            System.out.println("errors inserting rows");
            RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            System.out.println("there were errors inserting rows to Kudu");
            System.out.println("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                System.out.println(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                System.out.println("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
    }


}
