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

public class BulkResponse {

  private static final BulkResponse SUCCESS_RESPONSE = new BulkResponse(true, false, "", "");

  public final boolean succeeded;
  public final boolean retriable;
  public final String errorInfo;
  public final String errorRecord;

  private BulkResponse(boolean succeeded, boolean retriable, String errorInfo, String errorRecord) {
    this.succeeded = succeeded;
    this.retriable = retriable;
    this.errorInfo = errorInfo;
    this.errorRecord = errorRecord;
  }

  public static BulkResponse success() {
    return SUCCESS_RESPONSE;
  }

  public static BulkResponse failure(boolean retriable,String errorInfo,String errorRecord) {
    return new BulkResponse(false, retriable, errorInfo, errorRecord);
  }

  public boolean isSucceeded() {
    return succeeded;
  }

  public boolean isRetriable() {
    return retriable;
  }

  public String getErrorInfo() {
    return errorInfo;
  }

}
