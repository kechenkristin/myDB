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

    void updateAggResult(Map<Field, Integer> aggResult, Field gbField, int value) {
        if (aggResult.containsKey(gbField)) {
            aggResult.put(gbField, calculateAggregation(aggResult.get(gbField), value));
        } else {
            aggResult.put(gbField, value);
        }
    }

    abstract int calculateAggregation(int existingValue, int newValue);
}



class CountHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        updateAggResult(aggResult, gbField, 1);
    }

    @Override
    int calculateAggregation(int existingValue, int newValue) {
        return existingValue + newValue;
    }
}

class SumHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        updateAggResult(aggResult, gbField, aggField.getValue());
    }

    @Override
    int calculateAggregation(int existingValue, int newValue) {
        return existingValue + newValue;
    }
}

class MaxHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        updateAggResult(aggResult, gbField, Math.max(aggResult.getOrDefault(gbField, Integer.MIN_VALUE), aggField.getValue()));
    }

    @Override
    int calculateAggregation(int existingValue, int newValue) {
        return Math.max(existingValue, newValue);
    }
}

class MinHandler extends AggHandler{
    @Override
    void handle(Field gbField, IntField aggField) {
        updateAggResult(aggResult, gbField, Math.min(aggResult.getOrDefault(gbField, Integer.MAX_VALUE), aggField.getValue()));
    }

    @Override
    int calculateAggregation(int existingValue, int newValue) {
        return Math.min(existingValue, newValue);
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

    @Override
    int calculateAggregation(int existingValue, int newValue) {
        return 0;
    }
}