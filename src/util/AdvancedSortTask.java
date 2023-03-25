package util;

import build.Edge;
import util.DualHeapComm3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class AdvancedSortTask implements Callable<List<DualHeapComm3>> {
    private   List<DualHeapComm3> heapType3 = new ArrayList<>();
    private  Set<Integer> value;
    public AdvancedSortTask(Set<Integer> value, int queryK, ArrayList<Double> sortedWeight, HashMap<Edge, int[][]> edgeAssoType3, Integer key) {
        this.heapType3.add(new DualHeapComm3(queryK, key, sortedWeight, edgeAssoType3, 2));
        this.heapType3.add(new DualHeapComm3(queryK, key, sortedWeight, edgeAssoType3, 3));
        this.value = value;
    }

    @Override
    public List<DualHeapComm3> call() throws Exception {
//
        this.heapType3.get(0).addAll(this.value);
        this.heapType3.get(1).addAll(this.value);
        return heapType3;
    }
}
