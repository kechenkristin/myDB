package simpledb.execution;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.*;
import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    // final static Logger logger = LoggerFactory.getLogger(Insert.class);

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private Tuple insertTuple;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        tid = t;
        this.child = child;
        this.tableId = tableId;
        tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"insertNums"});
        insertTuple = null;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (insertTuple != null) {
            return null;
        }
        BufferPool bufferPool = Database.getBufferPool();
        int insertTuples = 0;
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(tid, tableId, child.next());
                insertTuples++;
            } catch (IOException e) {
                // logger.error(e.getMessage());
            }
        }
        insertTuple = new Tuple(this.tupleDesc);
        insertTuple.setField(0, new IntField(insertTuples));
        return insertTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}