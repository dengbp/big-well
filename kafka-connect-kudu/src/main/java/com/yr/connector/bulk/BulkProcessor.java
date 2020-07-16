package com.yr.connector.bulk;

import com.yr.kudu.utils.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.client.KuduClient;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Description todo
 * @Author DENGBP
 * @Date 15:02 2020-05-22
 **/
@Slf4j
public class BulkProcessor {

    private static final AtomicLong BATCH_ID_GEN = new AtomicLong();
    private final Time time;
    private final BulkClient<BulkRequest, BulkResponse> bulkClient;
    private final int maxBufferedRecords;
    /** 缓存最大记录数 */
    private final int batchSize;
    /** 最大滞留时间(毫秒) */
    private final long lingerMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final BehaviorOnException behaviorOnException;

    private final Thread daemonThread;
    private final ExecutorService executor;

    private volatile boolean stopRequested = false;
    private volatile boolean flushRequested = false;
    private final AtomicReference<ConnectException> error = new AtomicReference<>();

    private final Deque<BulkRequest> unsentRecords;
    private int inSendingRecords = 0;
    private final LogContext logContext = new LogContext();
    private final KuduClient client;


    /**
     * Description 初始化处理任务
     * @param time
     * @param bulkClient
     * @param maxBufferedRecords
     * @param batchSize
     * @param lingerMs
     * @param maxRetries
     * @param retryBackoffMs
     * @param behaviorOnException
     * @param client
     * @return
     * @Author dengbp
     * @Date 16:00 2020-05-22
     **/


    public BulkProcessor(
            Time time,
            BulkClient<BulkRequest, BulkResponse> bulkClient,
            int maxBufferedRecords,
            int batchSize,
            long lingerMs,
            int maxRetries,
            long retryBackoffMs,
            BehaviorOnException behaviorOnException, KuduClient client) {
        this.time = time;
        this.bulkClient = bulkClient;
        this.maxBufferedRecords = maxBufferedRecords;
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.behaviorOnException = behaviorOnException;
        unsentRecords = new ArrayDeque<>(maxBufferedRecords);
        this.client = client;
        final ThreadFactory threadFactory = buildThreadFactory();
        daemonThread = threadFactory.newThread(daemonTask());
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);
    }

    private ThreadFactory buildThreadFactory() {
        final AtomicInteger threadCounter = new AtomicInteger();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
                new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        log.error("Uncaught exception， thread={}", t, e);
                        failAndStop(e);
                    }
                };
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final int threadSeq = threadCounter.getAndIncrement();
                final int objHashCode = System.identityHashCode(this);
                final Thread t = new BulkProcessorThread(logContext, r, objHashCode, threadSeq);
                t.setDaemon(true);
                t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                return t;
            }
        };
    }

    private Runnable daemonTask() {
        return new Runnable() {
            @Override
            public void run() {
                log.info("Starting daemon task thread");
                try {
                    while (!stopRequested) {
                        submitBatchWhenReady();
                    }
                } catch (InterruptedException e) {
                    throw new ConnectException(e);
                }
                log.debug("Finished daemon task thread");
            }
        };
    }

    synchronized Future<BulkResponse> submitBatchWhenReady() throws InterruptedException {
        /** 判断记录滞留时间 */
        for (long waitStartTimeMs = time.milliseconds(), elapsedMs = 0;
             !stopRequested && !canSubmit(elapsedMs);
             elapsedMs = time.milliseconds() - waitStartTimeMs) {
            wait(Math.max(0, lingerMs - elapsedMs));
        }
        return stopRequested ? null : submitBatch();
    }

    private synchronized Future<BulkResponse> submitBatch() {
        final int numUnsentRecords = unsentRecords.size();
        assert numUnsentRecords > 0;
        final int batchAbleSize = Math.min(batchSize, numUnsentRecords);
        final List<BulkRequest> batch = new ArrayList<>(batchAbleSize);
        for (int i = 0; i < batchAbleSize; i++) {
            batch.add(unsentRecords.removeFirst());
        }
        inSendingRecords += batchAbleSize;
        log.info(
                "Submitting batch of {} records; {} unsent and {} total in-sending records",
                batchAbleSize,
                numUnsentRecords,
                inSendingRecords
        );
        return executor.submit(new BulkTask(batch));
    }

    /**
     * Description 判断滞留时间
     * @param elapsedMs 滞留时间
     * @return boolean
     * @Author dengbp
     * @Date 16:05 2020-05-22
     **/

    private synchronized boolean canSubmit(long elapsedMs) {
        return !unsentRecords.isEmpty() && (flushRequested || elapsedMs >= lingerMs || unsentRecords.size() >= batchSize);
    }

    public void start() {
        daemonThread.start();
    }

    public void stop() {
        log.debug("stop");
        stopRequested = true;
        synchronized (this) {
            executor.shutdown();
            notifyAll();
        }
    }

    public void awaitStop(long timeoutMs) {
        assert stopRequested;
        try {
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                throw new ConnectException("Timed-out waiting for executor termination");
            }
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Description todo
     * @param
     * @return boolean
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public boolean isStopping() {
        return stopRequested;
    }

    /**
     * Description todo
     * @param
     * @return boolean
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public boolean isFailed() {
        return error.get() != null;
    }

    /**
     * Description todo
     * @param
     * @return boolean
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public boolean isTerminal() {
        return isStopping() || isFailed();
    }

    /**
     * Description todo
     * @param
     * @return void
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public void throwIfStopping() {
        if (stopRequested) {
            throw new ConnectException("Stopping");
        }
    }

    /**
     * Description todo
     * @param
     * @return void
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public void throwIfFailed() {
        if (isFailed()) {
            throw error.get();
        }
    }

    /**
     * Description todo
     * @param
     * @return void
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/

    public void throwIfTerminal() {
        throwIfFailed();
        throwIfStopping();
    }

    /**
     * Description todo
     * @param record
     * @param timeoutMs
     * @return void
     * @Author dengbp
     * @Date 15:58 2020-05-19
     **/
    public synchronized void add(SinkRecord record, long timeoutMs) {
        throwIfTerminal();

        int numBufferedRecords = bufferedRecords();
        if (numBufferedRecords >= maxBufferedRecords) {
            log.info(
                    "Buffer full at {} records, so waiting up to {} ms before adding",
                    numBufferedRecords,
                    timeoutMs
            );
            final long addStartTimeMs = time.milliseconds();
            for (long elapsedMs = time.milliseconds() - addStartTimeMs;
                 !isTerminal() && elapsedMs < timeoutMs && bufferedRecords() >= maxBufferedRecords;
                 elapsedMs = time.milliseconds() - addStartTimeMs) {
                try {
                    wait(timeoutMs - elapsedMs);
                } catch (InterruptedException e) {
                    throw new ConnectException(e);
                }
            }
            throwIfTerminal();
            if (bufferedRecords() >= maxBufferedRecords) {
                log.error("Add timeout expired before buffer availability,record={}",record.value().toString());
                throw new ConnectException("Add timeout expired before buffer availability");
            }
            log.info(
                    "Adding record to queue after waiting {} ms",
                    time.milliseconds() - addStartTimeMs
            );
        } else {
            log.trace("Adding record to queue");
        }
        if (record.value()==null){
            log.warn("!!! one record value is null from binlog in topic={}",record.topic());
            return;
        }
        BulkRequest request = new BulkRequest(client, record.value().toString());
        unsentRecords.addLast(request);
        notifyAll();
    }

    /**
     * Description todo
     * @param timeoutMs
     * @return void
     * @Author dengbp
     * @Date 15:59 2020-05-19
     **/


    public void flush(long timeoutMs) {
        final long flushStartTimeMs = time.milliseconds();
        try {
            flushRequested = true;
            synchronized (this) {
                notifyAll();
                for (long elapsedMs = time.milliseconds() - flushStartTimeMs;
                     !isTerminal() && elapsedMs < timeoutMs && bufferedRecords() > 0;
                     elapsedMs = time.milliseconds() - flushStartTimeMs) {
                    wait(timeoutMs - elapsedMs);
                }
                throwIfTerminal();
                if (bufferedRecords() > 0) {
                    throw new ConnectException("Flush timeout expired with unflushed records: "
                            + bufferedRecords());
                }
            }
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        } finally {
            flushRequested = false;
        }
        log.info("Flushed bulk processor (total time={} ms)", time.milliseconds() - flushStartTimeMs);
    }


    private static final class BulkProcessorThread extends Thread {

        private final LogContext parentContext;
        private final int threadSeq;

        public BulkProcessorThread(
                LogContext parentContext,
                Runnable target,
                int objHashCode,
                int threadSeq
        ) {
            super(target, String.format("BulkProcessor@%d-%d", objHashCode, threadSeq));
            this.parentContext = parentContext;
            this.threadSeq = threadSeq;
        }

        @Override
        public void run() {
            super.run();
        }
    }

    private final class BulkTask implements Callable<BulkResponse> {

        final long batchId = BATCH_ID_GEN.incrementAndGet();

        final List<BulkRequest> batch;

        BulkTask(List<BulkRequest> batch) {
            this.batch = batch;
        }

        @Override
        public BulkResponse call() throws Exception {
            final BulkResponse rsp;
            try {
                rsp = execute();
            } catch (Exception e) {
                failAndStop(e);
                throw e;
            }
            onBatchCompletion(batch.size());
            return rsp;
        }

        private BulkResponse execute() throws Exception {
            final long startTime = System.currentTimeMillis();
            final int maxAttempts = maxRetries + 1;
            for (int attempts = 1, retryAttempts = 0; true; ++attempts, ++retryAttempts) {
                boolean retriable = true;
                try {
                    final BulkResponse bulkRsp = (BulkResponse) bulkClient.execute(batch);
                    log.info("Executing batchId: {} of {} records with attempt: {}/{},use time:{} ms",
                            batchId, batch.size(), attempts, maxAttempts,(System.currentTimeMillis()-startTime));
                    if (bulkRsp.succeeded){
                        return bulkRsp;
                    }else {
                        retry(retriable, attempts, retryAttempts, maxAttempts,bulkRsp.getErrorInfo());
                    }
                } catch (Exception e) {
                    retry(retriable, attempts, retryAttempts, maxAttempts,e.getMessage());
                }
            }
        }

        private void retry(boolean retriable,int attempts,int retryAttempts,int maxAttempts,String errorInfo) throws Exception {
            if (retriable && attempts < maxAttempts) {
                long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(retryAttempts,
                        retryBackoffMs);
                log.warn("Failed to execute batch {} of {} records with attempt {}/{}, "
                                + "will attempt retry after {} ms. Failure reason: {}",
                        batchId, batch.size(), attempts, maxAttempts, sleepTimeMs, errorInfo);
                time.sleep(sleepTimeMs);
            } else {
                log.error("Failed to execute batch {} of {} records after total of {} attempt(s)",
                        batchId, batch.size(), attempts, errorInfo);
                throw new Exception(errorInfo);
            }
        }

        /**
         * Description  异常处理策略
         * @param bulkRsp
         * @return void
         * @Author dengbp
         * @Date 16:36 2020-05-22
         **/

        private void handleExceptionRecord(BulkResponse bulkRsp) {
            switch (behaviorOnException) {
                case IGNORE:
                    log.debug("当前异常发生，采用忽略方式. 异常信息： {}",
                            batchId, batch.size(), bulkRsp.getErrorInfo());
                    return;
                case WARN:
                    log.warn("当前异常发生，采用告警方式. 异常信息： {}",
                            batchId, batch.size(), bulkRsp.getErrorInfo());
                    return;
                case FAIL:
                    throw new ConnectException("Bulk request failed: " + bulkRsp.getErrorInfo());
                default:
                    throw new RuntimeException(String.format(
                            "Unknown value for %s enum: %s",
                            BehaviorOnException.class.getSimpleName(),
                            behaviorOnException
                    ));
            }
        }
    }

    private boolean responseContainsException(BulkResponse bulkRsp) {
        return bulkRsp.getErrorInfo().contains("mapper_parsing_exception")
                || bulkRsp.getErrorInfo().contains("illegal_argument_exception")
                || bulkRsp.getErrorInfo().contains("record_insert/update/delete_exception");
    }

    private synchronized void onBatchCompletion(int batchSize) {
        inSendingRecords -= batchSize;
        assert inSendingRecords >= 0;
        notifyAll();
    }

    private void failAndStop(Throwable t) {
        error.compareAndSet(null, toConnectException(t));
        stop();
    }

    public synchronized int bufferedRecords() {
        return unsentRecords.size() + inSendingRecords;
    }

    private static ConnectException toConnectException(Throwable t) {
        if (t instanceof ConnectException) {
            return (ConnectException) t;
        } else {
            return new ConnectException(t);
        }
    }

    public enum BehaviorOnException {
        IGNORE,
        WARN,
        FAIL;

        public static final BehaviorOnException DEFAULT = FAIL;

        public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
            private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

            @Override
            public void ensureValid(String name, Object value) {
                if (value instanceof String) {
                    value = ((String) value).toLowerCase(Locale.ROOT);
                }
                validator.ensureValid(name, value);
            }

            @Override
            public String toString() {
                return validator.toString();
            }

        };

        public static String[] names() {
            BehaviorOnException[] behaviors = values();
            String[] result = new String[behaviors.length];

            for (int i = 0; i < behaviors.length; i++) {
                result[i] = behaviors[i].toString();
            }

            return result;
        }

        public static BehaviorOnException forValue(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
