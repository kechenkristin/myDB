package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;

import java.util.HashMap;
import java.util.Map;

public abstract class AggHandler {
    // 存储字段对应的聚合结果
    Map<Field, Integer> aggResult;
    // gbField 用于分组的字段， aggField 现阶段聚合结果
    abstract void handle(Field gbField, IntField aggField);

    public AggHandler(){
        aggResult = new HashMap<>();
    }

    public Map<Field, Integer> getAggResult() {
        return aggResult;
    }
}

class CountHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        if(aggResult.containsKey(gbField)){
            aggResult.put(gbField, aggResult.get(gbField) + 1);
        }
        else{
            aggResult.put(gbField, 1);
        }
    }
}

class SumHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        int value = aggField.getValue();
        if(aggResult.containsKey(gbField)){
            aggResult.put(gbField, aggResult.get(gbField) + value);
        }
        else{
            aggResult.put(gbField, value);
        }
    }
}

class MaxHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        int value = aggField.getValue();
        if(aggResult.containsKey(gbField)){
            aggResult.put(gbField,Math.max(aggResult.get(gbField), value));
        }
        else{
            aggResult.put(gbField, value);
        }
    }
}

class MinHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        int value = aggField.getValue();
        if(aggResult.containsKey(gbField)){
            aggResult.put(gbField,Math.min(aggResult.get(gbField), value));
        }
        else{
            aggResult.put(gbField, value);
        }
    }
}

class AvgHandler extends AggHandler{
    Map<Field, Integer> sum = new HashMap<>();
    Map<Field, Integer> count = new HashMap<>();
    @Override
    void handle(Field gbField, IntField aggField) {
        int value = aggField.getValue();
        // 求和 + 计数
        if(sum.containsKey(gbField) && count.containsKey(gbField)){
            sum.put(gbField, sum.get(gbField) + value);
            count.put(gbField, count.get(gbField) + 1);
        }
        else{
            sum.put(gbField, value);
            count.put(gbField, 1);
        }
        aggResult.put(gbField, sum.get(gbField) / count.get(gbField));
    }
}