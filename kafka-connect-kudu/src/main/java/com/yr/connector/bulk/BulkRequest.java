/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.yr.connector.bulk;


import lombok.Data;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;


/**
 * Description todo
 * @Author dengbp
 * @Date 16:50 2020-05-19
 **/

@Data
public class BulkRequest {

    private final KuduClient client;


    private final String values;

    public BulkRequest(KuduClient client, String values) {
        this.client = client;
        this.values = values;
    }
}
