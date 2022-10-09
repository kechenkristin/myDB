package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
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

    File diskFile;

    TupleDesc schema;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        diskFile = f;
        schema = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return diskFile;
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
        return diskFile.getAbsoluteFile().hashCode();
    }


    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    private int pageSize() {
        return BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = pageSize();
        long offset = pid.getPageNumber() * pageSize;
        byte[] buffer = new byte[pageSize];

        try {
            RandomAccessFile rFile = new RandomAccessFile(diskFile, "r");
            rFile.seek(offset);
            for (int i = 0; i < pageSize; i++) {
                buffer[i] = (byte) rFile.read();
            }

            HeapPage heapPage = new HeapPage((HeapPageId) pid, buffer);
            rFile.close();
            return heapPage;
        } catch (Exception e) {
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile rFile = new RandomAccessFile(diskFile, "rw");

        PageId pid = page.getId();
        int pageSize = pageSize();
        int offset = pageSize * pid.getPageNumber();

        rFile.seek(offset);
        rFile.write(page.getPageData());
        rFile.close();

        page.markDirty(false, null);

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) ((diskFile.length() - 1 + pageSize()) / pageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {

        // fields
        Integer pgCount;
        TransactionId tid;
        Iterator<Tuple> tupleIterator;
        final int pageNum;
        final int tableId;

        // constructor
        public HeapFileIterator(TransactionId tid) {
            pgCount = null;
            this.tid = tid;
            tupleIterator = null;
            pageNum = numPages();
            tableId = getId();
        }

        private PageId getPageId(int pgC) {
            return new HeapPageId(tableId, pgC);
        }

        private Iterator<Tuple> getTupleIterator(int pgC) throws DbException, TransactionAbortedException {
            PageId pid = getPageId(pgC);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return p.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCount = 0;
            tupleIterator = getTupleIterator(pgCount);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (pgCount != null) {
                while (pgCount < pageNum - 1) {
                    if (tupleIterator.hasNext()) {
                        return true;
                    } else {
                        pgCount ++;
                        tupleIterator = getTupleIterator(pgCount);
                    }
                }
                return tupleIterator.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return tupleIterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCount = null;
            tupleIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

