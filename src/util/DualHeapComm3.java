package util;


import build.Edge;


import java.util.*;

public class DualHeapComm3 {
    private Integer queryK;
    private Integer start;
    private PriorityQueue<Integer> kMinHeap;
    private PriorityQueue<Integer> OtherMaxHeap;
    //    private ArrayList<PriorityQueue<Integer>> OtherMaxHeap;
    private List<Double> weight;
    private Map<Edge, int[][]> edgeAssoType3 = new HashMap<Edge, int[][]>();
    public Map<Double, Set<Integer>> edgeAsso;
    public Set<Double> weightSet = new LinkedHashSet<>();
    public ArrayList<Double> weightList = new ArrayList<>();
    private ArrayList<Integer> OtherList;
    private int type;

    public DualHeapComm3(Integer k, Integer start, List<Double> weight, Map<Edge, int[][]> edgeAssoType3, int type) {
        this.queryK = k;
        this.start = start;
        this.kMinHeap = new PriorityQueue<Integer>(new minComparator());
        this.OtherMaxHeap = new PriorityQueue<Integer>(new maxComparator());
        this.weight = weight;
        this.edgeAssoType3 = edgeAssoType3;
        this.type = type;
        this.OtherList = new ArrayList<>();
        this.edgeAsso = new HashMap<>();
    }

    public void changeEdge2Weight(Map<String, Double> edge2Weight) {
    }

    public double getQueryKWeight() {

        int cnt = 0;
        for (int i = 0; i < weightList.size(); i++) {
            cnt += edgeAsso.get(weightList.get(i)).size();
            if (cnt >= queryK) {
                return weightList.get(i);
            }
        }
        System.out.println("不可能");
        return 0.0;
    }


    private double getWeight(Integer peek) {
        double value = -1;
        int keyId = -1;
        Edge edge = new Edge(new int[]{start, peek});
        if (!edgeAssoType3.containsKey(edge)){
            System.out.println("sdsdsdsdsd");
        }
        for (int[] key : edgeAssoType3.get(edge)) {
            if (weight.get(key[type - 2]) > value) {
                keyId = key[type - 2];
                value = weight.get(keyId);
            }
        }
        return weight.get(keyId);
    }

    public void addAll(Set<Integer> value) {
        OtherList.addAll(value);
        OtherList.sort(new maxComparator());
        for (int key : OtherList) {
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
        OtherList.clear();
    }


    class maxComparator implements Comparator<Integer> {


        public int compare(Integer a, Integer b) {
            double weightS1 = getWeight(a);
            double weightS2 = getWeight(b);

            return weightS1 >= weightS2 ? -1 : 1;
        }
    }

    class minComparator implements Comparator<Integer> {


        public int compare(Integer a, Integer b) {
            double weightS1 = getWeight(a);
            double weightS2 = getWeight(b);
            return weightS1 < weightS2 ? -1 : 1;
        }
    }


    public Integer getQueryKNode() {
        assert kMinHeap.peek() != null;
        return kMinHeap.peek();
    }

    public Integer size() {
        return kMinHeap.size() + OtherMaxHeap.size();
    }

    public void remove(Integer key) {
        double weight = getWeight(key);
        edgeAsso.get(weight).remove(key);
        if (edgeAsso.get(weight).isEmpty()) {
            edgeAsso.remove(weight);
            weightList.remove(weight);
        }
    }


    public DualHeapComm3 deepCopy() {
        DualHeapComm3 dest = new DualHeapComm3(this.queryK, this.start, this.weight, this.edgeAssoType3, this.type);
        dest.kMinHeap.addAll(this.kMinHeap);
        dest.OtherMaxHeap.addAll(this.OtherMaxHeap);
        return dest;
    }
}
