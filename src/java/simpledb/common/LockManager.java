package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    /* 记录某个页面是否被加锁了,如果被加锁了,那么是哪些事务加的锁 */
    private Map<PageId, Map<TransactionId, PageLock>> pageLockMap;

    public LockManager() {
        pageLockMap = new ConcurrentHashMap<>();
    }

    // TODO: rewrite comments
    public synchronized boolean acquireLock(PageId pageId, TransactionId tid, int acquireType) throws TransactionAbortedException {
        final String lockTypeStr = PageLock.lockTypeString(acquireType);
        final String threadName = Thread.currentThread().getName();

        // 获取当前页面上已经添加的锁
        Map<TransactionId, PageLock> transactionPageLockMap = pageLockMap.get(pageId);

        // 1. 当前page还没有加过锁
        if (transactionPageLockMap== null || transactionPageLockMap.size() == 0) {
            PageLock pageLock = new PageLock(acquireType, tid);
            transactionPageLockMap = new ConcurrentHashMap<>();
            transactionPageLockMap.put(tid, pageLock);
            pageLockMap.put(pageId, transactionPageLockMap);
            System.out.println(threadName + ": the " + pageId + "has no lock, transaction require" + lockTypeStr + "successfully");
            return true;
        }

        // 2. 当前page有加过锁
        // 获取当前事务在当前页面上加的锁
        PageLock transactionLock = transactionPageLockMap.get(tid);
        // 2.1 当前事务在当前page上加过锁了
        if (transactionLock != null) {
            // 2.1.1 Read Lock
            if (acquireType == PageLock.READ) {
                System.out.println(threadName + ": the" + pageId + "have read lock with the same tid, transaction " + tid + " require" + lockTypeStr + " success");
                return true;
            }

            // 2.1.2 Write Lock
            if (acquireType == PageLock.WRITE) {
                // 存在其他事务在当前页面上加了读锁 --> 直接抛出异常,防止等待锁升级产生死锁问题
                if (transactionPageLockMap.size() > 1) {
                    System.out.println(threadName + ": the" + pageId + "have many read locks, transaction " + tid + " require" + lockTypeStr + " fail");
                    throw new TransactionAbortedException();
                }
                // 当前页面只存在当前事务加的读锁--进行锁升级
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

        if (transactionLock == null) {
        }
        return true;
    }
}
