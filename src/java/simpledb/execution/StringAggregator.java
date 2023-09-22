package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    @Serial
    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    Map<Field, Integer> aggResult;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(!what.equals(Op.COUNT)){
            throw new IllegalArgumentException("String类型只支持计数");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        aggResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbFiled = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        // 聚合值 由于是字符串，这里是计数，没有任何使用
        if(aggResult.containsKey(gbFiled)){
            aggResult.put(gbFiled, aggResult.get(gbFiled) + 1);
        }
        else{
            aggResult.put(gbFiled, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        // 储存结果
        List<Tuple> tuples = new ArrayList<>();
        if(gbfield == NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(aggResult.get(null)));
            tuples.add(tuple);
        }else{
            tupleDesc = IntegerAggregator.getTupleDesc(aggResult, tuples, gbfieldtype);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
