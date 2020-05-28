package kudu;

import com.yr.connector.bulk.BulkRequest;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author dengbp
 * @ClassName DequeTest
 * @Description TODO
 * @date 2020-05-27 16:44
 */
public class ArrayDequeDequePerformanTest {

    public static void main(String[] args) {
        int size = 5000000;
        Deque<BulkRequest> unsentRecords = new ArrayDeque(size);
        for (int i=0;i<size;i++){
            String iStr = Integer.toString(i);
            BulkRequest request = new BulkRequest(null,"", iStr);
            unsentRecords.addLast(request);
        }

        for (int i=0;i<Runtime.getRuntime().availableProcessors();i++){

            Thread threadA = new Thread(new Consumer(unsentRecords, size));
            threadA.start();
        }

//        System.out.println(unsentRecords.size());
//        AtomicInteger inSendingRecords = new AtomicInteger(0);
//        inSendingRecords.addAndGet(100);
//        int batchSize = 12;
//        System.out.println(inSendingRecords.addAndGet(-batchSize));
    }

    static class Producer implements Runnable{
        private final Deque<BulkRequest> deque;

        Producer(Deque<BulkRequest> deque) {
            this.deque = deque;
        }


        public void putData(){

        }

        @Override
        public void run() {
            putData();
        }
    }


    static class Consumer implements Runnable{
        private final Deque<BulkRequest> deque;
        private final int buffer;

        Consumer(Deque<BulkRequest> deque, int buffer) {
            this.deque = deque;
            this.buffer = buffer;
        }


        public synchronized  void getData(){
            long start = System.currentTimeMillis();
            for (int i=0;i<buffer;i++) {
                synchronized (deque) {
                    if (!deque.isEmpty()) {
                        System.out.println("thread id=" + Thread.currentThread().getId() + ",values:" + deque.removeFirst().getValues());
                    }
                }
            }
            System.out.println("消费完毕,用时："+ (System.currentTimeMillis() - start));
        }

        @Override
        public void run() {
            getData();
        }
    }
}
