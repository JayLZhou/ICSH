package util;

import build.Edge;

import java.util.*;

public class DualHeap_Copy {

    private Integer queryK;
    private Integer start;
    private PriorityQueue<Integer> kMinHeap;
    private PriorityQueue<Integer> OtherMaxHeap;
    private Map<Edge, Double> edge2Weight;

    public DualHeap_Copy(Integer k, Map<Edge, Double> edge2Weight, Integer start) {
        this.queryK = k;
        this.edge2Weight = edge2Weight;
        this.start = start;
        this.kMinHeap = new PriorityQueue<Integer>(new minComparator(start));
        this.OtherMaxHeap = new PriorityQueue<Integer>(new maxComparator(start));
    }

    class maxComparator implements Comparator<Integer> {
        private int key;

        public maxComparator(Integer key_) {
            this.key = key_;
        }

        public int compare(Integer a, Integer b) {
            double weightS1 = getWeight(a);
            double weightS2 = getWeight(b);
            return weightS1 >= weightS2 ? -1 : 1;
        }
    }

    class minComparator implements Comparator<Integer> {
        private int key;

        public minComparator(Integer key_) {
            this.key = key_;
        }

        public int compare(Integer a, Integer b) {
            double weightS1 = getWeight(a);
            double weightS2 = getWeight(b);
            return weightS1 < weightS2 ? -1 : 1;
        }
    }

    public Set<Integer> getMinSet() {
        Set<Integer> newSet = new HashSet<Integer>();
        for (int key : kMinHeap) {
            newSet.add(key);
        }
        return newSet;
    }

    public double getQueryKWeight() {
        return getWeight(getQueryKNode());
    }

    public Set<Integer> getTopK(int k, Set<Integer> aliveSet) {
        Set<Integer> topkSet = new LinkedHashSet<Integer>();
        Set<Integer> minkSet = new LinkedHashSet<Integer>();
        Set<Integer> maxkSet = new LinkedHashSet<Integer>();
        while (k > 0) {
            if (kMinHeap.isEmpty()) {
                int key = OtherMaxHeap.poll();
                maxkSet.add(key);
                if (!aliveSet.contains(key)) {
                    k = k - 1;
                    topkSet.add(key);

                }
            } else {
                int key = kMinHeap.poll();
                minkSet.add(key);
                if (!aliveSet.contains(key)) {
                    k = k - 1;
                    topkSet.add(key);
                }
            }
        }
        kMinHeap.addAll(minkSet);
        OtherMaxHeap.addAll(maxkSet);
        return topkSet;
    }

    public Set<Integer> getGreaterWeightSet(double gate) {
        Set<Integer> greaterSet = new HashSet<Integer>();
        Set<Integer> delSet = new HashSet<Integer>();
        if (getQueryKWeight() >= gate) {
            greaterSet.addAll(getMinSet());
            while (!OtherMaxHeap.isEmpty() && getWeight(OtherMaxHeap.peek()) >= gate) {
                int key = OtherMaxHeap.poll();
                greaterSet.add(key);
                delSet.add(key);
            }
        }
        OtherMaxHeap.addAll(delSet);
        return greaterSet;
    }

    public Integer getQueryKNode() {
        return kMinHeap.peek();
    }

    public Integer size() {
        return kMinHeap.size() + OtherMaxHeap.size();
    }

    public void remove(Integer key) {
        if (!kMinHeap.contains(key)) {
            OtherMaxHeap.remove(key);
            return;
        }
        kMinHeap.remove(key);
        if (OtherMaxHeap.size() > 0) {
            kMinHeap.add(OtherMaxHeap.poll());
        }
    }

    public double getWeight(Integer key) {
        String str;
        if (this.start < key) {
            str = this.start + ":" + key;
        } else {
            str = key + ":" + this.start;
        }
        double weightStr = 0.0;
        if (edge2Weight.containsKey(str)) {
            weightStr = edge2Weight.get(str);
        }
        return weightStr;
    }

    public void add(Integer key) {
        if (kMinHeap.size() < queryK) {
            kMinHeap.add(key);
        } else {
            if (getWeight(kMinHeap.peek()) < getWeight(key)) {
                Integer k = kMinHeap.poll();
                OtherMaxHeap.add(k);
                kMinHeap.add(key);
            } else {
                OtherMaxHeap.add(key);
            }
        }
    }
}
