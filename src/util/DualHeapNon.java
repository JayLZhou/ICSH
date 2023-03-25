package util;

import build.Edge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DualHeapNon {
    public Map<Integer, Set<Integer>> edgeAsso = new HashMap<>();
    public ArrayList<Integer> weightList = new ArrayList<>();
    public double[]weight;

//    public DualHeap(Integer k, Map<Edge, Integer> edge2Weight, List<Double> weight, Integer start) {
//
//        this.edge2Weight = edge2Weight;
//        this.start = start;
//        this.weight = weight;
//    }

    public DualHeapNon(ArrayList<Integer> keyList, Map<Integer, Set<Integer>> verAsso, double[]weight) {
        edgeAsso = verAsso;
        weightList = keyList;
        this.weight = weight;

    }

    public DualHeapNon(int queryK, Map<Edge, Integer> edge2Weight, ArrayList<Double> sortedWeight, Integer key) {
    }


    public double getQueryKWeight(Integer queryK) {
        int cnt = 0;
        for (int i = 0; i < weightList.size(); i++) {
            cnt += edgeAsso.get(weightList.get(i)).size();
            if (cnt >= queryK) {
                return weight[weightList.get(i)];
            }
        }
        System.out.println("impossible");
        return 0.0;
    }

    public void remove(Integer key, Integer weight) {
        if (!edgeAsso.containsKey(weight)) {
            System.out.println("sdsdsdsdsd");
        }
        edgeAsso.get(weight).remove(key);
        if (edgeAsso.get(weight).isEmpty()) {
            edgeAsso.remove(weight);
            weightList.remove(weight);
        }
    }

    public void addAll(Set<Integer> value) {
    }
}
