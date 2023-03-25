package advanced;

import basic.Comm3Type;
import build.BatchSearchImproved;
import build.FastBCore;
import build.Edge;
import build.HomoGraphBuilderImprved;
import javafx.util.Pair;
import util.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class GetHofHPlus {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    public HashMap<Edge, int[][]> edgeAssoType3 = new HashMap<Edge, int[][]>();
    public int[] id2KeyId = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    public Map<Integer, Integer> indexK = new HashMap<Integer, Integer>();
    public List<double[]> cornerBound = new ArrayList<>();
    long findOtherTypeTime = 0;
    long findThisTypeTime = 0;
    long mergeIntervalTime = 0;
    double f4 = 0;
    Sort sort = new Sort();
    deepCopy deepCopy = new deepCopy();
    List<Double> sortedType3Weight = new ArrayList<>();
    List<Double> sortedType2Weight = new ArrayList<>();
    Map<Integer, List<DualHeapComm3>> pnb2WeightHeap = new HashMap<Integer, List<DualHeapComm3>>();
    public ArrayList<Double> sortedWeight = new ArrayList<>();
    long vistedMaps = 0;
    int res_cnt = 0;
    long GraphSize = 0;
    Set<Integer> sortedType1 = new LinkedHashSet<Integer>();

    public GetHofHPlus(double[] weight, int queryK, Set<Integer> sortedType1, List<Integer> sortedType2, List<Integer> sortedType3, double f4, Map<Integer, Set<Integer>> tmpMap, HashMap<Edge,int[][]> edgeAssoType3) {
        this.weight= weight;
        this.queryK = queryK;
        this.sortedType1 = deepCopy.copy(sortedType1);
        this.pnbMap = deepCopy.copyMap(tmpMap);
        this.edgeAssoType3 = edgeAssoType3;
        for (double value : weight) {
            sortedWeight.add(value);
        }
        this.f4 = f4;
    }


    public Double getWeight(int start, int end, int type) {

        double value = -1;
        int keyId = -1;
        Edge edge = new Edge(new int[]{start, end});
        for (int[] key : edgeAssoType3.get(edge)) {
            if (weight[key[type - 2]] > value) {
                keyId = key[type - 2];
                value = weight[keyId];
            }
        }
        return weight[keyId];
    }


    public Map<Integer, List<DualHeapComm3>> sorted4WeightHeap(Map<Integer, Set<Integer>> pnbMap) throws InterruptedException, ExecutionException {
        Map<Integer, List<DualHeapComm3>> pnb2Weight = new HashMap<Integer, List<DualHeapComm3>>();
        ExecutorService executor = Executors.newFixedThreadPool(18);
        Queue<Pair<Integer, Future<List<DualHeapComm3>>>> futureList = new LinkedList<>();

        for (Integer key : pnbMap.keySet()) {
            Set<Integer> value = pnbMap.get(key);
            vistedMaps += value.size();
            Future<List<DualHeapComm3>> heap = executor.submit(new AdvancedSortTask(value, queryK, sortedWeight, edgeAssoType3, key));
            futureList.add(new Pair<Integer, Future<List<DualHeapComm3>>>(key, heap));
        }
        while (!futureList.isEmpty()) {
            Pair<Integer, Future<List<DualHeapComm3>>> next = futureList.poll();
            int key = next.getKey();
            Future<List<DualHeapComm3>> heap = next.getValue();
            if (heap.isDone()) {
                pnb2Weight.put(key, heap.get());
            } else {
                futureList.add(next);
            }

        }
        executor.shutdown();
        return pnb2Weight;
    }

    public GetHofHPlus(double weight[],
                       int queryK, MetaPath queryMPath, HashMap<Edge, int[][]> edgeAssoType3 ) {

        this.weight = weight;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        this.sortedIndex = sort.sortedIndex(weight);
        for (double value : weight) {
            sortedWeight.add(value);
        }
        this.edgeAssoType3 = edgeAssoType3;
    }

    public Map<double[], Set<Integer>> computeComm() throws ExecutionException, InterruptedException {


        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        int minVer = 0;

        long startSort = System.currentTimeMillis();
        pnb2WeightHeap = sorted4WeightHeap(pnbMap);
        long endSort = System.currentTimeMillis();
        System.out.println("sort time is : " + (endSort - startSort) + ":ms");
        tmpMap = deepCopy.copyMap(pnbMap);
        ArrayList<Integer> cvs = new ArrayList<Integer>();
        Deque<Integer> keys = new ArrayDeque<Integer>();
        ArrayList<Pair<Integer, Double[]>> conditionInf = new ArrayList<Pair<Integer, Double[]>>();
//            List<Double[]> maxTop = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        while (!sortedType1.isEmpty()) {
            minVer = sortedType1.iterator().next();
            keys.add(minVer);
            Queue<Integer> queue = new LinkedList<Integer>();
            Set<Integer> minVerSet = tmpMap.get(minVer);
            double maxType2 = getConditionMaxValue(minVer, minVerSet, 2);
            double maxType3 = getConditionMaxValue(minVer, minVerSet, 3);
            conditionInf.add(new Pair<>(minVer, new Double[]{maxType2, maxType3}));

//                type2Top.add(maxType2);
//                type3Top.add(maxType3);
            queue.offer(minVer);
            while (!queue.isEmpty()) {
                int v = queue.poll();
                Set<Integer> nbSet = tmpMap.get(v);
                for (int i : nbSet) {
                    Set<Integer> pnb = tmpMap.get(i);
                    if (pnb.size() == queryK) {
                        queue.offer(i);
                    }
                    pnb.remove(v);
                    pnb2WeightHeap.get(i).get(0).remove(v);
                    pnb2WeightHeap.get(i).get(1).remove(v);
                    tmpMap.replace(i, pnb);
                }
                pnb2WeightHeap.remove(v); // todo
                tmpMap.remove(v);
                cvs.add(v);
                sortedType1.remove(v);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("get f1 and f2 time is : " + (endTime - startTime) + "ms");
        int len = conditionInf.size();
        this.cornerBound.clear();
        this.tmpMap.clear();
        this.pnb2WeightHeap.clear();
        int idx = 0;
        for (int i = len - 1; i >= 0; i--) {
            // new early stop.
            List<Set<Integer>> allCommunity = new ArrayList<>();
            System.out.println("current idx is : " + idx + "|" + "len is :" + len);
            idx += 1;
            int u = conditionInf.get(i).getKey();
            double type3Value = conditionInf.get(i).getValue()[1];
            double type2Value = conditionInf.get(i).getValue()[0];
//                if (type2Value <= type2Top.peek() && type3Value <= type3Top.peek()) {
//                    break;
//                }
            int indexU = cvs.indexOf(u);
            List<double[]> allInfluence = new ArrayList<>();
            Set<int[]> deleteTTSet = new HashSet<>();
            Set<int[]> deleteSTSet = new HashSet<>();
            keys.pollLast();
//                type2Top.remove(type2Value);
//                type3Top.remove(type3Value);
            if (checkDomain(type2Value, type3Value)) {
                System.out.println("you");
                continue;
            }
            buildMap(indexU, cvs, tmpMap, keys);
            sortedType2Weight.clear();
            sortedType3Weight.clear();
            double f1 = weight[u];
            System.out.println("f1 = " + f1);
            Set<Integer> community = new HashSet<Integer>();
            // 1. find 最大的f3 和 最小的f2
            Map<Integer, Set<Integer>> localTTMap = deepCopy.copyMap(tmpMap);
            // first find bound
            for (Integer key : tmpMap.keySet()) {
                vistedMaps += (tmpMap.get(key).size() * 1L);
            }
            type3Value = findCommunityAndValue(localTTMap, community, 3, u, type3Value);
            if (community.isEmpty()) {
                System.out.println("you");
                continue;
            }
            double minType2Value = findOtherValue(deepCopy.copyMap(localTTMap), type3Value, 2, u, community); //
            Map<Integer, Set<Integer>> searchMap = deepCopy.copyMap(tmpMap);

            deleteTTSet = cleanMap(searchMap, 3, type3Value);
            boolean falgA = true;
            if (!checkDomain(minType2Value, type3Value)) {
                allCommunity.add(new HashSet<>(community));
                allInfluence.add(new double[]{minType2Value, type3Value});
                falgA = false;
            }
            // 2. find最大的f2和最小的f3
            community.clear();
            Map<Integer, Set<Integer>> localSTMap = deepCopy.copyMap(tmpMap);
            boolean flagB = true;
            type2Value = findCommunityAndValue(localSTMap, community, 2, u, type2Value);
            if (community.isEmpty()) {
                System.out.println("you");
                continue;
            }
            double minType3Value = findOtherValue(deepCopy.copyMap(localSTMap), type2Value, 3, u, community);
//                Map<Integer, Set<Integer>> searchSTMap = deepCopy.copyMap(tmpMap);
//                deleteSTSet = cleanMap(searchSTMap, 2, type2Value);
            if (!checkDomain(type2Value, minType3Value)) {
                allCommunity.add(new HashSet<>(community));
                if (type2Value != minType2Value || type3Value != minType3Value) {
                    allInfluence.add(new double[]{type2Value, minType3Value});
                }
                flagB = false;
            }

            if (allInfluence.size() == 1 && !falgA && !flagB) {
                updateCorner(allInfluence);
                deleteMapAndChangeWeight(tmpMap);
                deleteMapAndChangeWeight(pnbMap);

                for (double[] influence : allInfluence) {
                    System.out.println("f1 is : " + weight[u] + "|" + Arrays.toString(influence));
                }
                result.put(new double[]{weight[u], allInfluence.get(0)[0], allInfluence.get(0)[1]}, allCommunity.get(0));
                continue;
            }
            for (int[] linked : deleteTTSet) {
                double Type3Weight = getWeight(linked[0], linked[1], 3);
                if (Type3Weight > minType3Value && Type3Weight < type3Value) {
                    sortedType3Weight.add(Type3Weight);
                }
            }

            Set<Double> distinctSorted2 = new HashSet<>(sortedType2Weight);
            Set<Double> distinctSorted3 = new HashSet<>(sortedType3Weight);
            sortedType2Weight.clear();
            sortedType2Weight.addAll(distinctSorted2);
            sortedType3Weight.clear();
            sortedType3Weight.addAll(distinctSorted3);
            sortedType3Weight.sort(new myComparator<>(weight));
            sortedType2Weight.sort(new myComparator<>(weight));
            long startSearch = System.currentTimeMillis();

            if (!sortedType3Weight.isEmpty()) {

                int leftType2 = 0;
                int rightType2 = sortedType2Weight.size() - 1;
                int leftType3 = 0;
                int rightType3 = sortedType3Weight.size() - 1;

                while (rightType3 >= 0 && minType2Value < type2Value) {
                    // use's (leftType3 + 1, rightType3 - 1) to find's valid community with max's t

                    Set<Integer> localComm = findCommunityWithBoundary(searchMap, sortedType3Weight, 3, u, rightType3, deleteTTSet);
                    Map<Integer, Set<Integer>> tmpLocalMap = deepCopy.copyMap(searchMap);
                    // second limit the boundary search
                    for (Integer key : tmpLocalMap.keySet()) {
                        vistedMaps += (tmpLocalMap.get(key).size() * 2L);
                    }
                    double newType2Min = findOtherValue(tmpLocalMap, sortedType3Weight.get(rightType3), 2, u, localComm);
                    if (newType2Min > minType2Value && newType2Min < type2Value) {
                        double newType3 = sortedType3Weight.get(rightType3);
                        if (!checkDomain(newType2Min, newType3)) {
                            allInfluence.add(new double[]{newType2Min, newType3});
                            allCommunity.add(new HashSet<>(localComm));
                            updateCorner(new double[]{newType2Min, newType3});
                            minType2Value = newType2Min;
                        }
                    }
                    rightType3 -= 1;

                }

            }
            long endSearch = System.currentTimeMillis();
            System.out.println("search All possible time is : " + (endSearch - startSearch) + ":ms");
            updateCorner(allInfluence);
            if (allInfluence.size() > 0) {
                for (double[] influence : allInfluence) {
                    System.out.println("f1 is : " + weight[u] + "|" + Arrays.toString(influence));
                }
            }

            deleteMapAndChangeWeight(tmpMap);
            deleteMapAndChangeWeight(pnbMap);
            for (int k = 0; k < allInfluence.size(); k++) {
                result.put(new double[]{ weight[u], allInfluence.get(k)[0], allInfluence.get(k)[1]}, allCommunity.get(k));
            }
        }


        System.out.println("find this time is : " + findThisTypeTime);
        System.out.println("find other time is : " + findOtherTypeTime);
        System.out.println("merge internal  time is : " + mergeIntervalTime);
        System.out.println("all visited graph size is  : " + vistedMaps);
        result = analysizeResult(result);
        Map<double[], Set<Integer>> newResult = new HashMap<>();
        Set<double[]> deleteSets = new HashSet<>();
        for (double[] influence : result.keySet()) {
            double[] newInfluence = new double[influence.length + 1];
            newInfluence[0] = f4;
            System.arraycopy(influence, 0, newInfluence, 1, newInfluence.length - 1);
            newResult.put(newInfluence, result.get(influence));
            deleteSets.add(influence);
        }
        return newResult;
    }

    public Map<double[], Set<Integer>> analysizeResult(Map<double[], Set<Integer>> Communities) {
        List<double[]> cornerBound = new ArrayList<>();

        Map<Double, List<double[]>> checkResult = new HashMap<>();
        Map<Pair<Double, Pair<Double, Double>>, Set<Integer>> res = new HashMap();
        Map<double[], Set<Integer>> validRes = new HashMap();
        Set<Double> type1Set = new HashSet<>();
        // store a map | key : x, value (y,z)
        for (Map.Entry<double[], Set<Integer>> entries : Communities.entrySet()) {
            double[] inf = entries.getKey();
            res.put(new Pair<>(inf[0], new Pair<>(inf[1], inf[2])), entries.getValue());
            type1Set.add(inf[0]);
            if (checkResult.containsKey(inf[0])) {
                checkResult.get(inf[0]).add(new double[]{inf[1], inf[2]});
            } else {
                List<double[]> tmpList = new ArrayList<>();
                tmpList.add(new double[]{inf[1], inf[2]});
                checkResult.put(inf[0], tmpList);
            }
        }
        int cnt = 0;
        // fix x and filter the y and z.
        // is not need ?
        Map<Double, List<double[]>> infMap = new HashMap<>();
        for (double key : checkResult.keySet()) {
//            Set<double[]> add = filter(checkResult.get(key));
//            assert add.size() == checkResult.size();
            List<double[]> addList = new ArrayList<>();
            addList.addAll(checkResult.get(key));
            infMap.put(key, addList);
        }
        List<Double> type1List = new ArrayList<>(type1Set);
        List<double[]> validKey = new ArrayList<>();
        type1List.sort(new myComparator(weight));
        for (int i = type1List.size() - 1; i >= 0; i--) {
            for (double[] inf : infMap.get(type1List.get(i))) {
                if (!checkDomain(cornerBound, inf[0], inf[1])) {
                    cnt += 1;
                    validKey.add(new double[]{type1List.get(i), inf[0], inf[1]});
                    Pair<Double, Pair<Double, Double>> keyPair = new Pair<>(type1List.get(i), new Pair<>(inf[0], inf[1]));
                    validRes.put(new double[]{type1List.get(i), inf[0], inf[1]}, res.get(keyPair));
                }
            }
            cornerBound = updateCorner(cornerBound, infMap.get(type1List.get(i)), weight);
        }
        for (double[] inf : validKey) {
            System.out.println(Arrays.toString(inf) + "｜" + res.get(new Pair<>(inf[0], new Pair<>(inf[1], inf[2]))).toString());
        }

        System.out.println("总结果个数为 cnt : " + cnt);
        return validRes;
    }

    public static Set<double[]> filter(List<double[]> alivePair) {
        alivePair.sort(new Comparator<double[]>() {
            @Override
            public int compare(double[] left, double[] right) {
                if (left[0] != right[0]) {
                    return left[0] < right[0] ? -1 : 1;
                } else {
                    return left[1] < right[1] ? -1 : 1;

                }
            }
        });
        Set<double[]> validPair = new HashSet<>();
        int length = alivePair.size() - 1;
        validPair.add(alivePair.get(length));
        double type2Limit = alivePair.get(length)[1];
        for (int i = length - 1; i >= 0; i--) {
            double[] curPair = alivePair.get(i);
            if (curPair[1] <= type2Limit) {
                continue;
            }
            type2Limit = curPair[1];
            validPair.add(curPair);
        }
        return validPair;
    }

    private Double getConditionMaxValue(int minVer, Set<Integer> minVerSet, int type) {
        PriorityQueue<Double> topK = new PriorityQueue<Double>();
        for (Integer tar : minVerSet) {
            double kWeight = pnb2WeightHeap.get(tar).get(type - 2).getQueryKWeight();
            if (topK.size() < queryK) {
                topK.add(kWeight);
            } else if (topK.size() == queryK) {
                if (!topK.isEmpty() && topK.peek() < kWeight) {
                    topK.poll();
                    topK.add(kWeight);
                }
            }
        }
        topK.add(pnb2WeightHeap.get(minVer).get(type - 2).getQueryKWeight());
        return topK.peek();
    }

    private void rebuildMap(int keyID, Map<Integer, Set<Integer>> tmpMap) {
        if (tmpMap.isEmpty()) {
            return;
        }
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();
        for (Map.Entry<Integer, Set<Integer>> entry : tmpMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyID) {
                tmpMap.clear();
                return;
            }
            Set<Integer> pnbSet = tmpMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = tmpMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
        for (int key : deleteSet) {
            tmpMap.remove(key);
        }
    }

    private void updateCorner(double[] newInfluence) {
        cornerBound.add(newInfluence);
        cornerBound.sort(new myComparator<>(weight));
        List<double[]> validBound = new ArrayList<>();
        int length = cornerBound.size() - 1;
        double type2Limit = cornerBound.get(length)[1];
        validBound.add(cornerBound.get(length));
        for (int i = length - 1; i >= 0; i--) {
            double[] curPair = cornerBound.get(i);
            if (curPair[1] <= type2Limit) {
                continue;
            }
            type2Limit = curPair[1];
            validBound.add(curPair);
        }
        validBound.sort(new myComparator<>(weight));
        cornerBound = validBound;
    }


    private Set<Integer> findCommunityWithBoundary(Map<Integer, Set<Integer>> localTTMap, List<Double> sortedType3Weight, int type, int keyID, int pos, Set<int[]> conditionAddSet) {
        Set<Integer> community = new HashSet<>();
        addEdgeAndfindCommunity(community, localTTMap, sortedType3Weight.get(pos), type, keyID, conditionAddSet, true);
        return community;
    }


    private boolean checkDomain(double type2Value, double type3Value) {

        if (cornerBound.isEmpty()) {
            return false;
        }
        int high = cornerBound.size() - 1;
        if (type2Value > cornerBound.get(high)[0]) {
            return false;
        }
        int low = 0;
        while (low < high) {
            int mid = (low + high) >> 1;
            if (cornerBound.get(mid)[0] >= type2Value) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return cornerBound.get(low)[1] >= type3Value;
    }

    private static boolean checkDomain(List<double[]> cornerBound, double type2Value, double type3Value) {
        if (cornerBound.isEmpty()) {
            return false;
        }
        int high = cornerBound.size() - 1;
        if (type2Value > cornerBound.get(high)[0]) {
            return false;
        }
        int low = 0;
        while (low < high) {
            int mid = (low + high) >> 1;
            if (cornerBound.get(mid)[0] >= type2Value) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return cornerBound.get(low)[1] >= type3Value;
    }

    private static List<double[]> updateCorner(List<double[]> cornerBound, List<double[]> newInfluence, double[] weight) {
        //        cornerBound.addAll(newInfluence);
        cornerBound.addAll(newInfluence);
        cornerBound.sort(new myComparator<>(weight));
        List<double[]> validBound = new ArrayList<>();
        int length = cornerBound.size() - 1;
        double type2Limit = cornerBound.get(length)[1];
        validBound.add(cornerBound.get(length));
        for (int i = length - 1; i >= 0; i--) {
            double[] curPair = cornerBound.get(i);
            if (curPair[1] <= type2Limit) {
                continue;
            }
            type2Limit = curPair[1];
            validBound.add(curPair);
        }
        validBound.sort(new myComparator<>(weight));
        return validBound;
    }

    private Double findCommunityAndValue(Map<Integer, Set<Integer>> tmpMap, Set<Integer> community, int type, int keyID, Double limitValue) {
        List<Double> values = new ArrayList<>();
        // 1. clean map to get the conditionAddSet
        Set<int[]> conditionAddSet = cleanMap(tmpMap, type, limitValue);
        for (int[] linked : conditionAddSet) {
            values.add(getWeight(linked[0], linked[1], type));
        }
//
        // clean values;
        Set<Double> distinct = new HashSet<>(values);
        values.clear();
        values.addAll(distinct);
        // sort values
        values.sort(new myComparator<>(weight));
        // 2. findCommunity
        checkCommunity(community, deepCopy.copyMap(tmpMap), keyID);
        Set<Integer> deleteSet = new HashSet<>();
        // 3. binary search
//        double upperValue = 0.0;
        if (community.isEmpty()) {
            if (values.isEmpty()) {
                return 0.0;
            }
            // binary search
            int l = 0;
            int r = values.size() - 1;
            while (l < r) {
                int mid = (l + r + 1) >> 1;
                limitValue = values.get(mid);
                Map<Integer, Set<Integer>> findMap = deepCopy.copyMap(tmpMap);
                addEdgeAndfindCommunity(community, findMap, limitValue, type, keyID, conditionAddSet, true);
                if (!community.isEmpty()) {
                    l = mid;
                } else {
                    r = mid - 1;
                }
            }
//            r = values.size() - 1;

            limitValue = values.get(l);
            deleteSet = addEdgeAndfindCommunity(community, tmpMap, limitValue, type, keyID, conditionAddSet, false);
//            System.out.println("1:" + limitValue  + "|" + deleteSet);
//            l = 0;
//            community.clear();
            // not binary search
//            while (community.isEmpty() && r >= l) {
//                limitValue = values.get(r);
//                deleteSet = addEdgeAndfindCommunity(community, tmpMap, limitValue,type, keyID, conditionAddSet, false);
//                r -= 1;
//            }
//            System.out.println("2:" + limitValue  + "|" + deleteSet);

        }
        for (int deleteId : deleteSet) {
            tmpMap.remove(deleteId);
        }
        return limitValue;
    }

    private Set<Integer> addEdgeAndfindCommunity(Set<Integer> community, Map<Integer, Set<Integer>> tmpMap, double upperValue, int type, int keyID, Set<int[]> conditionAddSet, boolean isDeepCopy) {
        //1. add edge
        addEdge(tmpMap, upperValue, type, conditionAddSet);
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

    private void addEdge(Map<Integer, Set<Integer>> localMap, double upperValue, int type, Set<int[]> conditionAddSet) {
        for (int[] edge : conditionAddSet) {
            int key = edge[0];
            int nb = edge[1];
            if (getWeight(key, nb, type) >= upperValue) {
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


    private void deleteMapAndChangeWeight(Map<Integer, Set<Integer>> tmpMap) {
        Set<int[]> deleteSet = new HashSet<>();
        for (int key : tmpMap.keySet()) {
            for (int val : tmpMap.get(key)) {
                if (key > val || !edgeAssoType3.containsKey(new Edge(key, val))) {
                    continue;
                }
                int[][] distinctPair = edgeAssoType3.get(new Edge(key, val));
//                distinctPair.removeIf(pair -> checkDomain(weight[pair[0]], weight[pair[1]]));
                List<int[]> newPair = new ArrayList<>();
                for (int[] pair : distinctPair) {
                    if (!checkDomain(weight[pair[0]], weight[pair[1]])) {
                        newPair.add(pair);
                    }
                }

                if (newPair.isEmpty()) {
                    deleteSet.add(new int[]{key, val});
                } else {
                    int[][] newArr = new int[newPair.size()][2];
                    for (int i = 0; i < newArr.length; i++) {

                        newArr[i] = newPair.get(i);
                    }
                    edgeAssoType3.replace(new Edge(key, val), newArr);
                }
            }
        }

        for (int[] edge : deleteSet) {
            int start = edge[0];
            int end = edge[1];
            edgeAssoType3.remove(new Edge(start, end));
            tmpMap.get(start).remove(end);
            tmpMap.get(end).remove(start);
            pnbMap.get(start).remove(end);
            pnbMap.get(end).remove(start);


        }

    }


    private double findOtherValue(Map<Integer, Set<Integer>> tmpMap, double tarValue,
                                  int type, int keyID, Set<Integer> community) {
        rebuildMap(keyID, tmpMap);
        List<int[][]> conditionValue = new ArrayList<>();
        for (int key : tmpMap.keySet()) {
            for (int val : tmpMap.get(key)) {
                conditionValue.add(getNewWeightID(key, val, tarValue, type));
            }
        }
        conditionValue.sort(new myComparator<>(weight));
        Set<Integer> keepSet = new HashSet<>(tmpMap.keySet());
        while (!keepSet.isEmpty()) {
            community.clear();
            community.addAll(keepSet);
            int start = conditionValue.get(0)[1][0];
            int end = conditionValue.get(0)[1][1];
            int valueId = conditionValue.get(0)[0][0];
            tmpMap.get(start).remove(end);
            tmpMap.get(end).remove(start);
            cleanMap(tmpMap, keepSet, keyID);
            if (keepSet.isEmpty()) {
                return weight[valueId];
            }
            conditionValue.remove(0);
        }
        return weight[conditionValue.get(0)[0][0]];
    }

    private int[][] getNewWeightID(int start, int end, double tarValue, int type) {
        int[][] keyId = new int[2][2];
        double value = -1;
        // type is 3 check pairArr[1]
        // type is 2 check pairArr[0]
        int[][] distinctPair = edgeAssoType3.get(new Edge(start, end));
        int newtype = (type == 3) ? 2 : 3;
        for (int[] pairArr : distinctPair) {
            if (weight[pairArr[newtype - 2]] >= tarValue) {
                if (weight[pairArr[type - 2]] > value) {
                    value = weight[pairArr[type - 2]];
                    keyId[0] = new int[]{pairArr[type - 2], 0};
                    keyId[1] = new int[]{start, end};
                }
            }
        }
        if (value == -1) {
            System.out.println("impossible");
        }
        return keyId;
    }

    private Set<int[]> cleanMap(Map<Integer, Set<Integer>> rebuildMap, int type, Double peek) {
        Set<int[]> deleteEdge = new HashSet<>();
        for (Map.Entry<Integer, Set<Integer>> entry : rebuildMap.entrySet()) {
            for (int value : entry.getValue()) {
                if (entry.getKey() > value) {
                    continue;
                }
                if (getWeight(entry.getKey(), value, type) < peek) {
                    deleteEdge.add(new int[]{entry.getKey(), value});
                }
            }

        }
        for (int[] delEdge : deleteEdge) {
            rebuildMap.get(delEdge[0]).remove(delEdge[1]);
            rebuildMap.get(delEdge[1]).remove(delEdge[0]);
        }
        return deleteEdge;
    }

    private void cleanMap(Map<Integer, Set<Integer>> tmpMap, Set<Integer> keepSet, int keyID) {
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();
        for (Map.Entry<Integer, Set<Integer>> entry : tmpMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyID) {
                keepSet.clear();
                return;
            }
            keepSet.remove(curId);

            Set<Integer> pnbSet = tmpMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    if (!tmpMap.containsKey(pnbId)) {
                        System.out.println("debug ");
                    }
                    Set<Integer> tmpSet = tmpMap.get(pnbId);

                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }

    }


    private void buildMap(int indexU, ArrayList<
            Integer> cvs, Map<Integer, Set<Integer>> tmpMap, Deque<Integer> keys) {
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
                if (edgeAssoType3.containsKey(new Edge(nb, v)) && !checkDomain(getWeight(nb, v, 2), getWeight(nb, v, 3))) {
                    tmpMap.get(nb).add(v);
                    tmpSet.add(nb);
                }
            }

            tmpMap.put(v, tmpSet);
        }

    }

    public void checkCommunity(Set<Integer> community, Map<Integer, Set<Integer>> tmpMap, int keyID) {
        community.addAll(tmpMap.keySet());
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();
        for (Map.Entry<Integer, Set<Integer>> entry : tmpMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyID) {
                community.clear();
                return;
            }
            community.remove(curId);
            Set<Integer> pnbSet = tmpMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = tmpMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
    }

    private void updateCorner(List<double[]> newInfluence) {
//        cornerBound.addAll(newInfluence);
        cornerBound.addAll(newInfluence);
        cornerBound.sort(new myComparator<>(weight));
        List<double[]> validBound = new ArrayList<>();
        int length = cornerBound.size() - 1;
        double type2Limit = cornerBound.get(length)[1];
        validBound.add(cornerBound.get(length));
        for (int i = length - 1; i >= 0; i--) {
            double[] curPair = cornerBound.get(i);
            if (curPair[1] <= type2Limit) {
                continue;
            }
            type2Limit = curPair[1];
            validBound.add(curPair);
        }
        validBound.sort(new myComparator<>(weight));
        cornerBound = validBound;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        int vertex1[] = {1, 0, 2, 0, 1}, edge1[] = {3, 1, 4, 0};
//        int vertex1[] = {1, 0, 3, 0, 1}, edge1[] = {3, 2, 5, 0};/**/
//        int vertex1[] = {3, 0, 1, 0, 3}, edge1[] = {5, 0, 3, 2}; // S-G-D-G-S
//        int vertex1[] = {3, 0, 2, 0, 3}, edge1[] = {5, 1, 4, 2}; // S-G-C-G-S
//        int vertex1[] = {1, 0, 2, 0, 1}, edge1[] = {1, 2, 3, 0}; // A-M-D-M-A for imdb
        String metapath = "47-83-422-746-51-83-422-746-47";
//        MetaPath metaPath1 = new MetaPath(vertex1, edge1);
        MetaPath metaPath1 = new MetaPath(metapath);
        DataReader dataReader = new DataReader(Config.dbpediaGraph, Config.dbpediaVertex, Config.dbpediaEdge, Config.dblpWeight);
//        DataReader dataReader = new DataReader(Config.tmdbGraph, Config.tmdbVertex, Config.tmdbEdge, Config.tmdbWeight);
//        DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
//        DataReader dataReader = new DataReader(Config.IMDBGraph, Config.IMDBVertex, Config.IMDBEdge, Config.IMDBWeight);

        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();

//        GetHofHPlus InfCommunities = new GetHofHPlus(graph, vertexType, edgeType, weight, 5, metaPath1);
//        Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
//        Communities.forEach((key, value) -> {
//            System.out.println(Arrays.toString(key) + "    " + value);
//        });
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}

