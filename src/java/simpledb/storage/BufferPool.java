package simpledb.storage;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {

    // final static Logger logger = LoggerFactory.getLogger(BufferPool.class);
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     */

    private final int numPages;
    private final Map<PageId, Page> pageCache;
    // for Lab2
    private final LRUEvict evict;
    // for lab3
    private final LockManager lockManager;

    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCache = new ConcurrentHashMap<>();
        this.evict = new LRUEvict(numPages);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    private DbFile getDbFile(int tableId) {
        return Database.getCatalog().getDatabaseFile(tableId);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        // lab5
        int acquireType = perm == Permissions.READ_WRITE ? PageLock.WRITE : PageLock.READ;

        long start = System.currentTimeMillis();
        long timeout = 500;

        boolean isAcquired = false;
        while (!isAcquired) {
            try {
                isAcquired = lockManager.acquireLock(pid, tid, acquireType);
            } catch (InterruptedException ignored) {
            }
            long now = System.currentTimeMillis();
            if(now-start > timeout){
                System.out.println("Deadlock exceeds time out!");
                throw new TransactionAbortedException();
            }
        }

//        while (true){
//            try{
//                if (lockManager.acquireLock(pid,tid,acquireType)){
//                    System.out.println("Lock manage successfully gets the lock!");
//                    break;
//                }
//            } catch (InterruptedException e){
//                // logger.error(e.getMessage());
//            }
//            long now = System.currentTimeMillis();
//            if(now-start > timeout){
//                System.out.println("Deadlock exceeds time out!");
//                throw new TransactionAbortedException();
//            }
//        }

        // lab3
        if(!pageCache.containsKey(pid)){
            DbFile dbFile = getDbFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            evict.modifyData(pid);
            if(pageCache.size() == numPages){
                evictPage();
            }
            pageCache.put(pid,page);
        }
        return pageCache.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.completeTransaction(tid);
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // return false;
        return lockManager.isHoldLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                // logger.error(e.getMessage());
            }
        } else {
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    private synchronized void recoverPages(TransactionId tid) {
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                int tableId = pid.getTableId();
                DbFile dbFile = getDbFile(tableId);
                Page cleanPage = dbFile.readPage(pid);
                pageCache.put(pid, cleanPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = getDbFile(tableId);
        updateBufferPool(dbFile.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = getDbFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(dbFile.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pages, TransactionId tid) throws DbException {
        for (Page page : pages) {
            page.markDirty(true, tid);
            if (pageCache.size() == numPages) {
                evictPage();
            }
            pageCache.put(page.getId(), page);
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page: pageCache.values()) {
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page flush = pageCache.get(pid);

        int tableId = pid.getTableId();
        DbFile dbFile = getDbFile(tableId);

        // append an update record to the log, with a before-image and after-image
        TransactionId dirtier = flush.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, flush.getBeforeImage(), flush);
            Database.getLogFile().force();
        }

        // 将page刷新到磁盘
        dbFile.writePage(flush);
        flush.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
       for (Page page : pageCache.values()) {
           if (tid.equals(page.isDirty())) {
               page.setBeforeImage();
               flushPage(page.getId());
           }
       }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
//         some code goes here
//         not necessary for lab1
        PageId evictPageId;
        Page page;
        boolean isAllDirty = true;
        for (int i = 0; i < pageCache.size(); i++) {
            evictPageId = evict.getEvictPageId();
            // page = pageCache.get(evictPageId);
            page = getDbFile(evictPageId.getTableId()).readPage(evictPageId);
            if (page.isDirty() != null) {
                evict.modifyData(evictPageId);
            } else {
                isAllDirty = false;
                discardPage(evictPageId);
                pageCache.remove(evictPageId);
                break;
            }
        }
        if (isAllDirty) {
            throw new DbException("The page in BufferPool is all dirty.");
        }
    }
}