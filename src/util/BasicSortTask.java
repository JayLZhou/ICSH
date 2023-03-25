package util;

import build.Edge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class BasicSortTask implements Callable<DualHeapComm3> {
    private   DualHeapComm3 heapType3 = null;
    private  Set<Integer> value;
    public BasicSortTask(Set<Integer> value, int queryK, ArrayList<Double> sortedWeight, HashMap<Edge, int[][]> edgeAssoType3, Integer key) {
        this.heapType3 = new DualHeapComm3(queryK, key, sortedWeight, edgeAssoType3, 2);
        this.value = value;
    }

    @Override
    public DualHeapComm3 call() throws Exception {
        this.heapType3.addAll(this.value);
        return heapType3;
    }
}
