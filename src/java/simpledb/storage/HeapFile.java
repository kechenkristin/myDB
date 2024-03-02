package simpledb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    final static Logger logger = LoggerFactory.getLogger(HeapFile.class);

    private File file;
    private TupleDesc tupleDesc;
    private BufferPool bufferPool;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        bufferPool = Database.getBufferPool();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();
        int offset = pageSize * pageNumber;

        Page page = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage(((HeapPageId) pid), data);
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        int offset = pageSize * pageNumber;

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long length = this.file.length();
        return ((int) Math.ceil(length * 1.0) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> modified = new ArrayList<>();
        for(int i=0; i<numPages(); i++){
            HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(this.getId(), i),Permissions.READ_WRITE);
            // when the slot is empty, can release the page into the slot.
            if (page.getNumEmptySlots() == 0){
                //当该page上没有空slot时，释放该page上的锁，避免影响其他事务的访问
                bufferPool.unsafeReleasePage(tid, page.getId());
                continue;
            }
            page.insertTuple(t);
            modified.add(page);
            return modified;
        }
        // when all pages are full, we need to create new page to let the file in
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        // add data at the end of the file
        outputStream.write(emptyPageData);
        outputStream.close();
        // load the page into cache, attention, the numpage()-1 is used because now a new page is created.
        HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> modified = new ArrayList<>();
        modified.add(page);
        return modified;
    }

    private class HeapFileIterator implements DbFileIterator {

        private static final long serialVersionUID = 1L;

        private int curPage = 0;
        private Iterator<Tuple> curItr = null;
        private TransactionId tid;
        private boolean open = false;;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            curPage = 0;
            if (curPage >= numPages()) {
                return;
            }
            curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), curPage), Permissions.READ_ONLY))
                    .iterator();
            advance();
        }

        private void advance() throws DbException, TransactionAbortedException {
            while (!curItr.hasNext()) {
                curPage++;
                if (curPage < numPages()) {
                    curItr = ((HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(getId(), curPage),
                            Permissions.READ_ONLY)).iterator();
                } else {
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException {
            if (!open) {
                return false;
            }
            return curPage < numPages();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if (!open) {
                throw new NoSuchElementException("iterator not open.");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples.");
            }
            Tuple result = curItr.next();
            advance();
            return result;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open) {
                throw new DbException("iterator not open yet.");
            }
            close();
            open();
        }

        @Override
        public void close() {
            curItr = null;
            curPage = 0;
            open = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

