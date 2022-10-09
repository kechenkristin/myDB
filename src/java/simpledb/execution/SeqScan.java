package simpledb.execution;

import org.hamcrest.core.StringStartsWith;
import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private static final long serialVersionUID = 1L;

    // fields
    int tableId;
    String tableAlias;
    TransactionId transactionId;
    DbFile file;
    DbFileIterator fileIter;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        tableId = tableid;
        this.tableAlias = tableAlias;
        transactionId = tid;
        file = getFile(tableId);
        fileIter = file.iterator(transactionId);
    }

    private DbFile getFile(int tableId) {
        return Database.getCatalog().getDatabaseFile(tableId);
    }


    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        // file = getFile(tableId);
        // fileIter = file.iterator(transactionId);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        fileIter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td = file.getTupleDesc();
        Type[] types = new Type[td.numFields()];
        String[] attr = new String[td.numFields()];

        String prefix = "null";

        if (tableAlias != null) {
            prefix = tableAlias;
        }

        for (int i = 0; i < td.numFields(); i++) {
            types[i] = td.getFieldType(i);
            attr[i] = prefix + "." + td.getFieldName(i);
        }

        return new TupleDesc(types, attr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return fileIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return fileIter.next();
    }

    public void close() {
        fileIter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        fileIter.rewind();
    }
}
