package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] histogram;
    private int numOfBuckets;
    private int max;
    private int min;
    private double width;
    private int numOfTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        numOfBuckets = buckets;
        this.min = min;
        this.max = max;
        width = (double) (max - min) / buckets;
        histogram = new int[numOfBuckets];
        numOfTuples = 0;
    }

    private int getIndex(int v) {
        if (v < min || v > max) throw new IllegalArgumentException("Value out of boundary!");
        if (v == max) return numOfBuckets -1;
        return (int) ((v - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = getIndex(v);
        histogram[index]++;
        numOfTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param constVal Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int constVal) {
        double selectivity = 0.0;

        switch (op) {
            case LESS_THAN:
                if (constVal <= min) return 0.0;
                if (constVal >= max) return 1.0;

                int index = getIndex(constVal);

                for (int i = 0; i < index; i++) {
                    selectivity += (histogram[i] + 0.0) /numOfTuples;
                }
                selectivity += histogram[index] * ((constVal - index * width - min) / width) / numOfTuples;
                return selectivity;

            case EQUALS:
                if (constVal < min || constVal > max) return 0.0;
                return 1.0 * histogram[getIndex(constVal)] / ((int) width + 1) / numOfTuples;

            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, constVal);

            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, constVal + 1);

            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, constVal);

            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, constVal - 1);

            default:
                throw new UnsupportedOperationException("Operation is illegal");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        double avg = 0.0;
        for (int i = 0; i < numOfBuckets; i++) {
            avg += (histogram[i] + 0.0) / numOfTuples;
        }
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < histogram.length; i++) {
            double b_l = i * width;
            double b_r = (i + 1) * width;
            sb.append(String.format("[%f, %f]: %d\n", b_l, b_r, histogram[i]));
        }

        return sb.toString();
    }
}
