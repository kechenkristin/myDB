package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;
    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final AggHandler aggHandler;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;

        switch (what) {
            case MIN -> aggHandler = new MinHandler();
            case MAX -> aggHandler = new MaxHandler();
            case AVG -> aggHandler = new AvgHandler();
            case SUM -> aggHandler = new SumHandler();
            case COUNT -> aggHandler = new CountHandler();
            default -> throw new IllegalArgumentException("Aggregator doesn't support this operator!");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField afield = (IntField) tup.getField(this.aField);
        Field gbfield = this.gbField == NO_GROUPING ? null : tup.getField(this.gbField);
        aggHandler.handle(gbfield, afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        Map<Field, Integer> aggResult = aggHandler.getAggResult();
        // 构建 tuple 需要
        TupleDesc tupleDesc;
        // 储存结果
        List<Tuple> tuples = new ArrayList<>();
        // 如果没有分组
        if(gbField == NO_GROUPING){
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            // 获取结果字段
            IntField resultField = new IntField(aggResult.get(null));
            // 组合成行（临时行，不需要存储，只需要设置字段值）
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }
        else{
            tupleDesc = getTupleDesc(aggResult, tuples, gbFieldType);
        }
        return new TupleIterator(tupleDesc ,tuples);
    }

    static TupleDesc getTupleDesc(Map<Field, Integer> aggResult, List<Tuple> tuples, Type gbFieldType) {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        for(Field field: aggResult.keySet()){
            Tuple tuple = new Tuple(tupleDesc);
            if(gbFieldType == Type.INT_TYPE){
                IntField intField = (IntField) field;
                tuple.setField(0, intField);
            }
            else{
                StringField stringField = (StringField) field;
                tuple.setField(0, stringField);
            }

            IntField resultField = new IntField(aggResult.get(field));
            tuple.setField(1, resultField);
            tuples.add(tuple);
        }
        return tupleDesc;
    }
}
