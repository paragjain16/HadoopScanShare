package teamdatum_test;

import teamdatum.FileHandle;
import teamdatum.FileObserver;
import teamdatum.FileService;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * File stats:
 *     Last line: aaaaa,bbbbb,ccccc,ddddd,eeeee,fffff,ggggg,hhhhh,iiiii,jjjjj,kkkkk,lllll,mmmmm,nnnnn,ooooo,ppppp,qqqqq,rrrrr,sssss,ttttt,uuuuu,vvvvv,wwwww,xxxxx,yyyyy,zzzzz
 *     Line length: 155
 *     Line count: 9699968
 */
public class Test {

    public static void main(String[] args)
        throws Exception
    {
        int concurrency = 20;

//        simpleTest(args[0]);
//        concurrentSimpleTest(args[0], concurrency);
//        simpleAsyncTest(args[0]);
        concurrentAsyncTestWithSharing(args[0], concurrency);
        FileService.singleton.shutdown();
    }

    private static void concurrentAsyncTestWithSharing(final String filePath, int concurrency)
        throws Exception
    {
        System.out.println("BEGIN: concurrentAsyncTestWithSharing");
        final CountDownLatch outerLatch = new CountDownLatch(concurrency);
        List<Thread> asyncTasks = new ArrayList<Thread>(concurrency);
        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrency; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        CountDownLatch latch = new CountDownLatch(1);
                        FileHandle fh = createFileObserver(filePath, latch);
                        System.out.println("BEGIN: [" + fh + "] concurrentAsyncTestWithSharing child task");
                        latch.await();
                        outerLatch.countDown();
                        System.out.println("END: [" + fh + "] concurrentAsyncTestWithSharing child task");
                    } catch (Exception exc) {
                        throw new RuntimeException(exc);
                    }
                }
            };
            Thread t = new Thread(r);
            t.start();
            asyncTasks.add(t);
        }
        outerLatch.await();
        long stop = System.currentTimeMillis();
        System.out.println("END: concurrentAsyncTestWithSharing (" + (stop - start) + ")");
    }

    private static void concurrentSimpleTest(final String filePath, final int concurrency)
        throws Exception
    {
        System.out.println("BEGIN: concurrentSimpleTest");
        final CountDownLatch outerLatch = new CountDownLatch(concurrency);
        List<Thread> asyncTasks = new ArrayList<Thread>(concurrency);
        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrency; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        BufferedReader br = new BufferedReader(new FileReader(filePath));
                        int count = 0;
                        String next = br.readLine();
                        while (next != null) {
                            count = count + 1;
                            next = br.readLine();
                        }
                        br.close();
                        outerLatch.countDown();
                        if (count != 9699968) throw new AssertionError("Concurrent simple test incorrect line count: " + count);
                    } catch (Exception exc) {
                        throw new RuntimeException(exc);
                    }
                }
            };
            Thread t = new Thread(r);
            t.start();
            asyncTasks.add(t);
        }
        outerLatch.await();
        long stop = System.currentTimeMillis();
        System.out.println("END: concurrentSimpleTest (" + (stop - start) + ")");
    }

    private static FileHandle createFileObserver(String filePath, final CountDownLatch latch)
        throws Exception
    {
        FileObserver fo = new FileObserver() {
            private long _start;
            private int _lineCount;
            private String _lastLine;
            @Override
            public void onClose(FileHandle handle) {
                long stop = System.currentTimeMillis();
                System.out.println("[" + handle + "] Async close -- duration: " + (stop - _start));
                if (_lineCount != 9699968) throw new AssertionError("[" + handle + "] Async incorrect line count: " + _lineCount);
                if (_lastLine.length() != 155) throw new AssertionError("[" + handle + "] Async incorrect line length");
                latch.countDown();
            }
            @Override
            public void onNext(FileHandle handle, String line) {
                _lineCount = _lineCount + 1;
                _lastLine = line;
            }
            @Override
            public void onOpen(FileHandle handle) {
                System.out.println("[" + handle + "] Async open");
                _start = System.currentTimeMillis();
            }
        };

        return FileService.singleton.open(filePath, fo);
    }

    private static FileHandle createNoOpObserver(String filePath)
            throws Exception
    {
        FileObserver fo = new FileObserver() {
            private int _lineCount;
            @Override
            public void onClose(FileHandle handle) {
                System.out.println("[" + handle + "] Async close -- total lines: " + _lineCount);
            }
            @Override
            public void onNext(FileHandle handle, String line) {
                _lineCount = _lineCount + 1;
            }
            @Override
            public void onOpen(FileHandle handle) {
                System.out.println("[" + handle + "] Async open");
            }
        };
        return FileService.singleton.open(filePath, fo);
    }

    private static void simpleAsyncTest(String filePath)
        throws Exception
    {
        System.out.println("BEGIN: simpleAsyncTest");
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(1);
        FileHandle fh = createFileObserver(filePath, latch);
        latch.await();
        long stop = System.currentTimeMillis();
        System.out.println("END: simpleAsyncTest (" + (stop - start) + ")");
    }

    private static void simpleTest(String filePath)
        throws Exception
    {
        System.out.println("BEGIN: simpleTest");
        long start = System.currentTimeMillis();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        int count = 0;
        String last = null;
        String next = br.readLine();
        while (next != null) {
            last = next;
            count = count + 1;
            next = br.readLine();
        }
        br.close();
        long stop = System.currentTimeMillis();
        System.out.println("Last line: " + last);
        System.out.println("Line length: " + last.length());
        System.out.println("Line count: " + count);
        System.out.println("END: simpleTest (" + (stop - start) + ")");
    }

}
