package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /* attributes for lab4 */
    private LockManager lockManager;

    private static class LinkNode {
        PageId pageId;
        Page page;
        LinkNode prev;
        LinkNode next;

        public LinkNode(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }

    /* attributes for evict */
    private Map<PageId, LinkNode> pageCache;
    private int capacity;
    private LinkNode head;
    private LinkNode tail;


    private void remove(LinkNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }



    private void addToHead(LinkNode node) {
        LinkNode oldFirst = head.next;
        head.next = node;
        node.prev = head;
        node.next = oldFirst;
        oldFirst.prev = node;
    }

    private LinkNode deleteLastNode() {
        LinkNode node = tail.prev;
        remove(node);
        return node;
    }

    private void moveNodeToHead(LinkNode node){
        remove(node);
        addToHead(node);
    }

    /**
     *
     * @return The size of current LRUCache
     */
    private int getSize() {
        return pageCache.size();
    }

    /**
     * returns true if the LRUCache is oversize
     */
    private boolean overSize() {
        return getSize() >= capacity;
    }

    /**
     * returns true if the cache contains the page with the given pageId
     */
    private boolean containsPage(PageId pageId) {
        return pageCache.containsKey(pageId);
    }

    private DbFile getDbFile(PageId pid) {
        return Database.getCatalog().getDatabaseFile(pid.getTableId());
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.capacity = numPages;
        pageCache = new ConcurrentHashMap<>(capacity);
        head = new LinkNode(new HeapPageId(-1, -1), null);
        tail = new LinkNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
        lockManager = new LockManager();
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
        throws TransactionAbortedException, DbException {

        // lab4: 在返回页面前阻塞并获取所需的锁
        int acquireType = perm == Permissions.READ_WRITE ? 1 : 0;

        long startTime = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;

        while (true) {
            try {
                if (lockManager.acquireLock(pid, tid, acquireType)) break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - startTime > timeout) throw new TransactionAbortedException();
        }

        if(!containsPage(pid)){
            DbFile dbFile = getDbFile(pid);
            Page page = dbFile.readPage(pid);

            if (overSize()) evictPage();

            LinkNode node = new LinkNode(pid, page);
            pageCache.put(pid, node);
            addToHead(node);
        }
        LinkNode node = pageCache.get(pid);
        moveNodeToHead(node);
        return node.page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pageId the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pageId) {
        lockManager.releaseLock(pageId, tid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pageCache.get(pid).page;

        if (page.isDirty() != null) {
            DbFile dbFile = getDbFile(pid);
            dbFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, LinkNode> entry : pageCache.entrySet()) {
            Page page = entry.getValue().page;
            page.setBeforeImage();
            if (page.isDirty() == tid) flushPage(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageCache.keySet()) {
            flushPage(pid);
        }
    }

    private void updateBufferPool(List<Page> pageList, TransactionId tid) throws DbException {
        for (Page page : pageList) {
            page.markDirty(true, tid);

            if (overSize()) evictPage();

            LinkNode node = pageCache.get(page.getId());
            node.page = page;
            pageCache.put(page.getId(), node);
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
        remove(pageCache.get(pid));
        pageCache.remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 回滚事务,从磁盘加载旧页完成回滚
            recoverPages(tid);
        }
        lockManager.completeTransaction(tid);
    }

    private synchronized void recoverPages(TransactionId tid) {
        for (Map.Entry<PageId, LinkNode> entry: pageCache.entrySet()) {
            PageId pageId = entry.getKey();
            Page page = entry.getValue().page;

            if (page.isDirty() == tid) {
                DbFile dbFile = getDbFile(pageId);
                Page cleanPage = dbFile.readPage(pageId);

                LinkNode node = pageCache.get(pageId);
                node.page = cleanPage;
                pageCache.put(pageId, node);
            }
        }
   }



    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pageId) {
        return lockManager.isHoldLock(pageId, tid);
    }



    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pageList = dbFile.insertTuple(tid, t);
        updateBufferPool(pageList, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
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
        DbFile dbFile = getDbFile(t.getRecordId().getPageId());
        List<Page> pageList = dbFile.deleteTuple(tid, t);
        updateBufferPool(pageList, tid);
    }



    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        /*
        LinkNode tail = deleteLastNode();
        PageId pid = tail.pageId;

        try {
            flushPage(pid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        pageCache.remove(pid);
         */

        for (int i = 0; i < capacity; i++) {
            LinkNode tail = deleteLastNode();
            Page evitPage = tail.page;

            if (evitPage.isDirty() != null) {
                // 不扔髒頁，提高髒頁優先級， 但不把髒頁寫回磁盤(flushPage())
                addToHead(tail);
            } else {
                // 扔乾淨頁
                PageId evictPageId = tail.pageId;
                discardPage(evictPageId);
                return;
            }
        }
        throw new DbException("All pages are dirty");
    }

}
