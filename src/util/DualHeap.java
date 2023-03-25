package util;

import build.Edge;

import java.util.*;

public class DualHeap {

    private Integer queryK;
    private Integer start;
    private Map<Edge, Integer> edge2Weight;
    private List<Double> weight = null;
    public Map<Double, Set<Integer>> edgeAsso = new HashMap<>();
    public Set<Double> weightSet = new LinkedHashSet<>();
    public ArrayList<Double> weightList = new ArrayList<>();
    private ArrayList<Integer> keysList = new ArrayList<>();
    private Set<Integer> keepSet = new HashSet<>();
//    private boolean first = true;

    public DualHeap(Integer k, Map<Edge, Integer> edge2Weight, List<Double> weight, Integer start) {
        this.queryK = k;
        this.edge2Weight = edge2Weight;
        this.start = start;
        this.weight = weight;
    }

    class maxComparator implements Comparator<Integer> {


        public int compare(Integer a, Integer b) {
            double weightS1 = getWeight(a);
            double weightS2 = getWeight(b);
            return weightS1 >= weightS2 ? -1 : 1;
        }
    }


    public double getQueryKWeight() {

        int cnt = 0;
        for (int i = 0; i < weightList.size(); i++) {
            cnt += edgeAsso.get(weightList.get(i)).size();
            if (cnt >= queryK) {
                return weightList.get(i);
            }
        }
//        return getWeight(keysList.get(queryK - 1));
        System.out.println("impossible");
        return 0.0;
    }

    public void remove(Integer key) {
        double weight = getWeight(key);
        edgeAsso.get(weight).remove(key);
        if (edgeAsso.get(weight).isEmpty()) {
            edgeAsso.remove(weight);
            weightList.remove(weight);
        }
//        keysList.remove(keysList.indexOf(key));
    }

    public double getWeight(Integer key) {
        Edge str = new Edge(this.start, key);
        double weightStr = 0.0;
        weightStr = weight.get(edge2Weight.get(str));
        return weightStr;
    }


    public void addAll(Set<Integer> value) {
        keysList.addAll(value);
        keysList.sort(new maxComparator());
//         sorted by the edge from big -> small
        for (int key : keysList) {
            double weight = getWeight(key);
            weightSet.add(weight);
            if (edgeAsso.containsKey(weight)) {
                edgeAsso.get(weight).add(key);
            } else {
                Set<Integer> tmpSet = new HashSet<>();
                tmpSet.add(key);
                edgeAsso.put(weight, tmpSet);
            }
        }
         // 去重
        weightList.addAll(weightSet);
        weightSet.clear();
        keysList.clear();
    }
}
