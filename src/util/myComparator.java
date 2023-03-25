package util;

import java.util.Comparator;

public class myComparator<T> implements Comparator<T> {
    private double[] weight = null;

    public myComparator(double[] weight) {
        this.weight = weight;
    }

    @Override
    public int compare(T left, T right) {
        double leftWeight = 0.0;
        double rightWeight = 0.0;
        if (left instanceof Integer) {
            leftWeight = weight[(Integer) left];
            rightWeight = weight[(Integer) right];
        } else if (left instanceof Double) {
            leftWeight = (Double) left;
            rightWeight = (Double) right;
        } else if (left instanceof double[]) {
            leftWeight = ((double[]) left)[0];
            rightWeight = ((double[]) right)[0];
        } else if (left instanceof int[][]) {
            leftWeight = weight[((int[][]) left)[0][0]];
            rightWeight = weight[((int[][]) right)[0][0]];
        }

        return leftWeight < rightWeight ? -1 : 1;
    }
}

