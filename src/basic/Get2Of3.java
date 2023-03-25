package basic;

import build.Edge;
import javafx.util.Pair;
import util.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Get2Of3 {
    private double weight[] = null;
    private int queryK = -1;

    Map<Integer, Set<Integer>> pnbMap, tmpMap, testMap, testMap2 = new HashMap<Integer, Set<Integer>>();
    Sort sort = new Sort();
    private double f3_weight;
    deepCopy deepCopy = new deepCopy();
    Map<Integer, DualHeapComm3> pnb2WeightHeap = new HashMap<Integer, DualHeapComm3>();
    Set<Integer> sortedType1 = new LinkedHashSet<Integer>();
    private ArrayList<Double> sortedWeight;
    public HashMap<Edge, int[][]> edgeAssoComm3 = new HashMap<Edge, int[][]>();
    private long vistedMaps = 0;

    int res_cnt = 0;

    public Get2Of3(HashMap<Edge, int[][]> edgeAssoComm3, ArrayList<Double> sortedWeight, int queryK, double f2, Map<Integer, Set<Integer>> pnbMap, Set<Integer> sortedtype1, double[] weight) throws ExecutionException, InterruptedException {
        this.queryK = queryK;
        this.f3_weight = f2;
        this.weight = weight;
        this.sortedWeight = sortedWeight;
        this.sortedType1 = new LinkedHashSet<>(sortedtype1);
        this.pnbMap = pnbMap;
        this.edgeAssoComm3 = edgeAssoComm3;
        this.vistedMaps = 0;
        pnb2WeightHeap = sorted4WeightHeap(pnbMap);
    }


    public Map<Integer, DualHeapComm3> sorted4WeightHeap(Map<Integer, Set<Integer>> pnbMap) throws ExecutionException, InterruptedException {
        Map<Integer, DualHeapComm3> pnb2Weight = new HashMap<Integer, DualHeapComm3>();
        ExecutorService executor = Executors.newFixedThreadPool(18);
        Queue<Pair<Integer, Future<DualHeapComm3>>> futureList = new LinkedList<>();

        for (Integer key : pnbMap.keySet()) {

            Set<Integer> value = pnbMap.get(key);
            vistedMaps += value.size();
            Future<DualHeapComm3> heap = executor.submit(new BasicSortTask(value, queryK, sortedWeight, edgeAssoComm3, key));
            futureList.add(new Pair<Integer, Future<DualHeapComm3>>(key, heap));
        }
        while (!futureList.isEmpty()) {
            Pair<Integer, Future<DualHeapComm3>> next = futureList.poll();
            int key = next.getKey();
            Future<DualHeapComm3> heap = next.getValue();
            if (heap.isDone()) {
                pnb2Weight.put(key, heap.get());
            } else {
                futureList.add(next);
            }

        }
        executor.shutdown();
        return pnb2Weight;
    }

    public Map<double[], Set<Integer>> computeComm() {
         int minVer = 0;
        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        tmpMap = deepCopy.copyMap(pnbMap);
        ArrayList<Integer> cvs = new ArrayList<Integer>();
        Deque<Integer> keys = new ArrayDeque<Integer>();
        ArrayList<Pair<Integer, Double>> conditionInf = new ArrayList<Pair<Integer, Double>>();

        while (!sortedType1.isEmpty()) {
            minVer = sortedType1.iterator().next();
            PriorityQueue<Double> topK = new PriorityQueue<>();
            Queue<Integer> queue = new LinkedList<Integer>();
            keys.add(minVer);
            Set<Integer> minVerSet = tmpMap.get(minVer);
            vistedMaps += minVerSet.size();
            for (Integer tar : minVerSet) {
                double kWeight = pnb2WeightHeap.get(tar).getQueryKWeight();
                if (topK.size() < queryK) {
                    topK.add(kWeight);
                } else if (topK.size() == queryK) {
                    if (!topK.isEmpty() && topK.peek() < kWeight) {
                        topK.poll();
                        topK.add(kWeight);
                    }
                }
            }
            topK.add(pnb2WeightHeap.get(minVer).getQueryKWeight());
            conditionInf.add(new Pair<>(minVer, topK.peek()));
            queue.offer(minVer);
            while (!queue.isEmpty()) {
                int v = queue.poll();
                for (int i : tmpMap.get(v)) {
                    Set<Integer> pnb = tmpMap.get(i);
                    vistedMaps += pnb.size();
                    if (pnb.size() == queryK) {
                        queue.offer(i);
                    }
                    pnb.remove(v);
                    pnb2WeightHeap.get(i).remove(v);
                    tmpMap.replace(i, pnb);
                }
                pnb2WeightHeap.remove(v); // todo
                tmpMap.remove(v);
                cvs.add(v);
                sortedType1.remove(v);
            }
        }
        tmpMap.clear();
        double last_f2 = 0.0;
        int len = conditionInf.size();
        for (int i = len - 1; i >= 0; i--) {
            // early stop

            int u = conditionInf.get(i).getKey();
            double f2_val = conditionInf.get(i).getValue();
            keys.pollLast();
            if (f2_val <= last_f2) {
                continue;
            }
            int indexU = cvs.indexOf(u);
            Set<int[]> deleteSet = new HashSet<>();
            List<Double> values = new ArrayList<>();

            Set<int[]> conditionSet = new HashSet<>();

            buildMap(indexU, cvs, tmpMap, f2_val, keys, conditionSet);
            for (int[] linked : deleteSet) {
                values.add(getWeight(linked[0], linked[1]));
            }
            // clean values;
            Set<Double> distinct = new HashSet<>(values);
            values.clear();
            values.addAll(distinct);
            // sort values
            values.sort(new myComparator<>(weight));
            Set<Integer> community = new HashSet<Integer>();
            double f1 = weight[u];
            for (Integer key : tmpMap.keySet()) {
                vistedMaps += tmpMap.get(key).size();
            }
            System.out.println("f1 = " + f1);
            findCommunity(community, u);
            if (community.isEmpty()) {
                List<Double> sortedType2Weight = new ArrayList<>();
                for (int edge[] : conditionSet) {
                    sortedType2Weight.add(getWeight(edge[0], edge[1]));
                }
                sortedType2Weight.sort(new myComparator<Double>(weight));
                int l = 0;
                int r = sortedType2Weight.size() - 1;
                while (l < r) {
                    int mid = (l + r + 1) >> 1;
                    f2_val = sortedType2Weight.get(mid);
                    Map<Integer, Set<Integer>> findMap = deepCopy.copyMap(tmpMap);
                    addEdgeAndfindCommunity(community, findMap, f2_val, u, conditionSet, true);
                    if (!community.isEmpty()) {
                        l = mid;
                    } else {
                        r = mid - 1;
                    }
                }
                f2_val = sortedType2Weight.get(l);
                System.out.printf("🌈 new f2 is %f: \n", f2_val);
            }
            if (f2_val <= last_f2) {
                continue;
            } else {
                addEdgeAndfindCommunity(community, tmpMap, f2_val, u, conditionSet, true);
            }
            last_f2 = f2_val;
//            cleanMap(last_f2);
            res_cnt++;
            System.out.println("f2 = " + f2_val);
            double influence[] = {f1, f2_val, f3_weight};
            System.out.println(Arrays.toString(influence) + "|" + "community is : " + community);
            result.put(influence, community);
        }

        System.out.println("The result size is  : 🎉" + res_cnt);
        return result;
    }

    private Set<Integer> addEdgeAndfindCommunity(Set<Integer> community, Map<Integer, Set<Integer>> tmpMap, double upperValue, int keyID, Set<int[]> conditionAddSet, boolean isDeepCopy) {
        //1. add edge
        addEdge(tmpMap, upperValue, conditionAddSet);
        //2. find community
        community.clear();
        community.addAll(tmpMap.keySet());
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();
        Map<Integer, Set<Integer>> localMap = new HashMap<>();
        if (isDeepCopy) {
            localMap = deepCopy.copyMap(tmpMap);
        } else {
            localMap = tmpMap;
        }
        for (Map.Entry<Integer, Set<Integer>> entry : localMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyID) {
                community.clear();
                return deleteSet;
            }
            community.remove(curId);
            Set<Integer> pnbSet = localMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = localMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
        return deleteSet;
    }

    private void addEdge(Map<Integer, Set<Integer>> localMap, double upperValue, Set<int[]> conditionAddSet) {
        for (int[] edge : conditionAddSet) {
            int key = edge[0];
            int nb = edge[1];
            if (getWeight(key, nb) >= upperValue) {
                if (localMap.containsKey(key)) {
                    localMap.get(key).add(nb);
                } else {

                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(nb);
                    localMap.put(key, tmpSet);
                }
                if (localMap.containsKey(nb)) {
                    localMap.get(nb).add(key);
                } else {
                    Set<Integer> tt = new HashSet<>();
                    tt.add(key);
                    localMap.put(nb, tt);

                }

            }
        }

    }

    private void cleanMap(double f2_val) {
        Set<Pair<Integer, Integer>> deleteSet = new HashSet<Pair<Integer, Integer>>();
        if (!tmpMap.isEmpty()) {
            for (int key : tmpMap.keySet()) {
                for (int val : tmpMap.get(key)) {
                    if (key > val) {
                        continue;
                    }
                    if (getWeight(key, val) <= f2_val) {
                        deleteSet.add(new Pair<>(val, key));
                    }
                }
            }
        }
        for (Pair<Integer, Integer> edge : deleteSet) {
            int start = edge.getKey();
            int end = edge.getValue();
            tmpMap.get(start).remove(end);
            tmpMap.get(end).remove(start);
        }
    }

    private double getWeight(int start, int end) {
        int type = 2;
        double value = -1;
        int keyId = -1;
        Edge edge = new Edge(new int[]{start, end});
        for (int[] key : edgeAssoComm3.get(edge)) {
            if (weight[key[type - 2]] > value) {
                keyId = key[type - 2];
                value = weight[keyId];
            }
        }
        return weight[keyId];
    }

    private int findPosition(double val, List<Double> values) {
        int l = 0, r = values.size() - 1;
        while (l < r) {
            int mid = (l + r) >> 1;
            if (values.get(mid) >= val) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        return l;
    }


    private void buildMap(int indexU, ArrayList<Integer> cvs, Map<Integer, Set<Integer>> tmpMap, double f2_val, Deque<Integer> keys, Set<int[]> conditionAddSet) {
        for (int j = indexU; j < cvs.size(); j++) {
            int v = cvs.get(j);
            if (j != indexU && keys.contains(v)) {
                break;
            }
            Set<Integer> nbSet = pnbMap.get(v);
            Set<Integer> tmpSet = new HashSet<Integer>();
            for (int nb : nbSet) {
                if (!tmpMap.containsKey(nb)) {
                    continue;
                }
                if (getWeight(nb, v) >= f2_val) {
                    tmpMap.get(nb).add(v);
                    tmpSet.add(nb);
                } else {
                    conditionAddSet.add(new int[]{v, nb});
                }
            }
            tmpMap.put(v, tmpSet);
        }
    }

    public void findCommunity(Set<Integer> community, int keyId) {
        Map<Integer, Set<Integer>> localMap = new HashMap<Integer, Set<Integer>>();
        localMap = deepCopy.copyMap(tmpMap);
        community.addAll(localMap.keySet());
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();

        for (Map.Entry<Integer, Set<Integer>> entry : localMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyId) {
                community.clear();
                return;
            }
            community.remove(curId);
            Set<Integer> pnbSet = localMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = localMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
    }

    public long getVistedMaps() {
        return this.vistedMaps;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//		int graph[][] = new int[11][];
//		int a0[] = {}; graph[0] = a0;
//		int a1[] = { 7, 1, 8, 2 }; graph[1] = a1;
//		int a2[] = { 7, 3, 8, 4 }; graph[2] = a2;
//		int a3[] = { 8, 5, 9, 6 }; graph[3] = a3;
//		int a4[] = { 7, 7, 9, 8 }; graph[4] = a4;
//		int a5[] = { 8, 9, 9, 10 }; graph[5] = a5;
//		int a6[] = { 10, 11 }; graph[6] = a6;
//		int a7[] = { 1, 12, 2, 13, 4, 14 }; graph[7] = a7;
//		int a8[] = { 1, 15, 2, 16, 3, 17, 5, 18 }; graph[8] = a8;
//		int a9[] = { 3, 19, 4, 20, 5, 21 }; graph[9] = a9;
//		int a10[] = { 6, 22 }; graph[10] = a10;
//		int vertexType[] = { 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0 };
//		int edgeType[] = { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
//		double weight[] = { 0, 2, 6, 5, 3, 1, 4, 60, 70, 80, 50 };
//        int vertex1[] = {1, 0, 1}, edge1[] = {3, 0};
        // for pubmed dataset
        // DGSGD
        int vertex1[] = {1, 0, 3, 0, 1}, edge1[] = {3, 1, 5, 0};

        MetaPath metaPath1 = new MetaPath(vertex1, edge1);

        DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }


}
