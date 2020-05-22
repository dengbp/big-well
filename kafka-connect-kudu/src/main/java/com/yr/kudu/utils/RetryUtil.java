package com.yr.kudu.utils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RetryUtil {

  public static final long MAX_RETRY_TIME_MS = TimeUnit.HOURS.toMillis(24);


  public static long computeRandomRetryWaitTimeInMillis(int retryAttempts,
                                                        long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts < 0) {
      return initialRetryBackoffMs;
    }
    long maxRetryTime = computeRetryWaitTimeInMillis(retryAttempts, initialRetryBackoffMs);
    return ThreadLocalRandom.current().nextLong(0, maxRetryTime);
  }


  public static long computeRetryWaitTimeInMillis(int retryAttempts,
                                                  long initialRetryBackoffMs) {
    if (initialRetryBackoffMs < 0) {
      return 0;
    }
    if (retryAttempts <= 0) {
      return initialRetryBackoffMs;
    }
    if (retryAttempts > 32) {
      return MAX_RETRY_TIME_MS;
    }
    long result = initialRetryBackoffMs << retryAttempts;
    return result < 0L ? MAX_RETRY_TIME_MS : Math.min(MAX_RETRY_TIME_MS, result);
  }


}
