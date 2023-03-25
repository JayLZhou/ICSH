package util;

import build.Edge;

import java.util.*;
import java.util.concurrent.Callable;

public class type2SortTask implements Callable<DualHeap> {
    private DualHeap heapType3 = null;
    private Set<Integer> value;


    public type2SortTask(Set<Integer> value, int queryK, ArrayList<Double> sortedWeight, Map<Edge, Integer> edge2Weight, Integer key) {
        this.heapType3 = new DualHeap(queryK, edge2Weight, sortedWeight, key);
        this.value = value;
    }

    @Override
    public DualHeap call() throws Exception {
        this.heapType3.addAll(this.value);
        return heapType3;
    }
}
