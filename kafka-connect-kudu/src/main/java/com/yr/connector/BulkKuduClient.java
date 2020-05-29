package com.yr.connector;

import com.yr.connector.bulk.BulkClient;
import com.yr.connector.bulk.BulkRequest;
import com.yr.connector.bulk.BulkResponse;
import com.yr.connector.bulk.KuduOperator;
import com.yr.kudu.session.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

/**
 * @author dengbp
 * @ClassName BulkKuduClient
 * @Description TODO
 * @date 2020-05-19 11:15
 */
@Slf4j
public class BulkKuduClient implements BulkClient<BulkRequest,BulkResponse> {

    private final KuduOperator kuduOperate;
    private final SessionManager sessionManager;


    public BulkKuduClient(KuduOperator kuduOperate, SessionManager sessionManager) {
        this.kuduOperate = kuduOperate;
        this.sessionManager = sessionManager;
    }



    @Override
    public BulkResponse execute(List<BulkRequest> reqs) throws IOException {
        final BulkResponse[] response = {null};
        KuduSession session = sessionManager.getSession();
        final int[] batch = {0};
        for (BulkRequest req : reqs){
            try {
                try {
                    kuduOperate.operation(session,req);
                } catch (ParseException e) {
                    e.printStackTrace();
                    log.error("Parse record exception error,message:{}",req.getValues());
                    response[0] = BulkResponse.failure(true,e.getMessage(),req.getValues());
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                batch[0]++;
                if (batch[0]>=SessionManager.OPERATION_BATCH/2){
                    session.flush();
                    batch[0] = 0;
                }
            } catch (KuduException e) {
                e.printStackTrace();
                log.error("insert message error,record:{}",req.getValues());
                response[0] = BulkResponse.failure(true,e.getMessage(),req.getValues());
               break;
            }
        };
        session.flush();
        session.close();
        log.info("flush into kudu dataBatch size={}",reqs.size());
        return  response[0]==null?BulkResponse.success():response[0];
    }
}
