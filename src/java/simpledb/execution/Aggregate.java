package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    int afield;
    int gfield;
    Aggregator.Op op;
    OpIterator opIterator;
    Aggregator aggregator;
    OpIterator executor;



    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    // TODO: fix this
    public TupleDesc getTupleDesc() {
        return opIterator.getTupleDesc();
    }

    /**
     * get field type to see whether it's int or string
     */
    private Type getFieldType() {
        return getTupleDesc().getFieldType(afield);
    }

    /**
     * get type for group by
     */
    private Type getGroupType() {
        return gfield == 1 ? null : getTupleDesc().getFieldType(gfield);
    }

    private void constructAggregator() {
        aggregator = ( getFieldType() == Type.INT_TYPE) ?
                new IntegerAggregator(gfield, getGroupType(), afield, op) :
                new StringAggregator(gfield, getGroupType(), afield, op);
    }

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.afield = afield;
        this.gfield = gfield;
        op = aop;

        opIterator = child;
        constructAggregator();
        executor = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return gfield == -1 ? getTupleDesc().getFieldName(gfield) : null;
    }


    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        opIterator.open();
        super.open();
        while (opIterator.hasNext()) {
            aggregator.mergeTupleIntoGroup(opIterator.next());
        }
        opIterator.close();
        executor = aggregator.iterator();
        executor.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (executor.hasNext()) {
            return executor.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        executor.rewind();
    }


    public void close() {
        super.close();
        opIterator = null;
        executor = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {this.opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        opIterator = children[0];
        constructAggregator();
    }

}
