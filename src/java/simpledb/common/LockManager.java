package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    /* 记录某个页面是否被加锁了,如果被加锁了,那么是哪些事务加的锁 */
    private final Map<PageId, Map<TransactionId, PageLock>> pageLockMap;

    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
    }

    // TODO: rewrite comments
    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, int acquireType) throws TransactionAbortedException, InterruptedException {
        final String lockTypeStr = PageLock.lockTypeString(acquireType);
        final String threadName = Thread.currentThread().getName();

        // 获取当前页面上已经添加的锁
        Map<TransactionId, PageLock> transactionPageLockMap = pageLockMap.get(pageId);

        // 1. 页面A上没有锁 -> 事物1直接加锁
        if (transactionPageLockMap== null || transactionPageLockMap.isEmpty()) {
            PageLock pageLock = new PageLock(acquireType, tid);
            transactionPageLockMap = new ConcurrentHashMap<>();
            transactionPageLockMap.put(tid, pageLock);
            pageLockMap.put(pageId, transactionPageLockMap);
            System.out.println(threadName + ": the " + pageId + "has no lock, transaction require" + lockTypeStr + "successfully");
            return true;
        }

        // 2. 页面A有加过锁
        // 获取当前事务在当前页面上加的锁
        PageLock transactionLock = transactionPageLockMap.get(tid);
        // 2.1 有事物1的锁
        if (transactionLock != null) {
            // 2.1.1 事物1请求的是Read Lock -》 事物1直接获取锁
            if (acquireType == PageLock.READ) {
                System.out.println(threadName + ": the" + pageId + "have read lock with the same tid, transaction " + tid + " require" + lockTypeStr + " success");
                return true;
            }

            // 2.1.2 事物1请求的是Write Lock
            if (acquireType == PageLock.WRITE) {
                // 存在其他事务在页面1上加了读锁 --> 直接抛出异常,防止等待锁升级产生死锁问题
                if (transactionPageLockMap.size() > 1) {
                    System.out.println(threadName + ": the" + pageId + "have many read locks, transaction " + tid + " require" + lockTypeStr + " fail");
                    throw new TransactionAbortedException();
                }
                // 页面1只存在事务A的读锁 --> 进行锁升级为写锁
                if (transactionPageLockMap.size() == 1 && transactionLock.getType() == PageLock.READ) {
                    transactionLock.setType(PageLock.WRITE);
                    transactionPageLockMap.put(tid, transactionLock);
                    pageLockMap.put(pageId, transactionPageLockMap);
                    System.out.println(threadName + ": the" + pageId + "have read lock with the same tid, transaction " +
                            tid + " require" + lockTypeStr + " success and upgrade");
                    return true;
                }
                //  当前事务在当前页面上已经加了写锁
                if (transactionPageLockMap.size() == 1 && transactionLock.getType() == PageLock.WRITE) {
                    System.out.println(threadName + ": the" + pageId + "have write lock with the same tid, transaction " + tid + " require" + lockTypeStr + " success");
                    return true;
                }
            }
        }

        // 没有事物1的锁
        if (transactionLock == null) {
            // 页面A上的锁大于1
            if (transactionPageLockMap.size() > 1) {
                // 事物1 请求读锁
                if (acquireType == PageLock.READ) {
                    PageLock tranLock = new PageLock(PageLock.READ, tid);
                    transactionPageLockMap.put(tid, tranLock);
                    pageLockMap.put(pageId, transactionPageLockMap);
                    System.out.println(threadName + ": the " + pageId + " have many read locks, transaction" + tid + " require " + lockTypeStr + ", accept and add a new read lock");
                    return true;
                }
                // 事物1请求写锁 -》 事物1 wait
                if (acquireType == PageLock.WRITE) {
                    wait(20);
                    System.out.println(threadName + ": the " + pageId + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                    return false;
                }
            }
                // 页面A上的锁等于1
            if (transactionPageLockMap.size() == 1) {
                PageLock curLock = transactionPageLockMap.entrySet().iterator().next().getValue();

                // 页面A上的锁是读锁
                if (curLock.getType() == PageLock.READ) {
                    // 事物1 请求读锁 -》 事物1 获取页面A上的读锁
                    if (acquireType == PageLock.READ) {
                        PageLock tranLock = new PageLock(PageLock.READ, tid);
                        transactionPageLockMap.put(tid, tranLock);
                        pageLockMap.put(pageId, transactionPageLockMap);
                        System.out.println(threadName + ": the " + pageId + " have one read lock with diff txid, transaction" + tid + " require read lock, accept and add a new read lock");
                        return true;
                    }

                    // 事物1 请求写锁 -》 事物1等待
                    if (acquireType == PageLock.WRITE) {
                        wait(10);
                        System.out.println(threadName + ": the " + pageId + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                        return false;
                    }
                }

                // 页面A上的锁是写锁
                if (curLock.getType() == PageLock.WRITE) {
                    // 如果是写锁
                    wait(10);
                    System.out.println(threadName + ": the " + pageId + " have one write lock with diff txid, transaction" + tid + " require read lock, await...");
                    return false;
                }
            }
        }
        System.out.println("---------------------");
        return false;
    }




    /**
     * 释放指定页面的指定事务加的锁
     *
     * @param pageId 页id
     * @param tid    事务id
     */
    public synchronized void releaseLock(PageId pageId, TransactionId tid) {
        if (tid == null) return;

        Map<TransactionId, PageLock> transactionIdPageLockMap = pageLockMap.get(pageId);
        if (transactionIdPageLockMap == null) return;

        PageLock transactionLock = transactionIdPageLockMap.get(tid);
        if (transactionLock == null) return;

        final String threadName = Thread.currentThread().getName();
        final String lockTypeStr = PageLock.lockTypeString(transactionLock.getType());

        transactionIdPageLockMap.remove(tid);
        System.out.println(threadName + " release " + lockTypeStr + " in " + pageId + ", the tid lock size is " + transactionIdPageLockMap.size());
        if (transactionIdPageLockMap.isEmpty()) {
            pageLockMap.remove(pageId);
            System.out.println(threadName + "release last lock, the page " + pageId + " have no lock, the page locks size is " + pageLockMap.size());
        }
        this.notifyAll();
    }

    /**
     * 判断事务是否持有对应页的锁
     *
     * @param pageId 页id
     * @param tid    事务id
     * @return 事务是否持有对应页的锁
     */
    public synchronized boolean isHoldLock(PageId pageId, TransactionId tid) {
        Map<TransactionId, PageLock> transactionIdPageLockMap = pageLockMap.get(pageId);
        if (transactionIdPageLockMap == null)  return false;
        return transactionIdPageLockMap.get(tid) != null;
    }

    public synchronized void completeTransaction(TransactionId tid) {
        Set<PageId> pageIdSet = pageLockMap.keySet();
        for (PageId pageId : pageIdSet) {
            releaseLock(pageId, tid);
        }
    }
}
