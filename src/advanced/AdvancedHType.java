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


public class AdvancedHType implements Comm3Type {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    public HashMap<Edge, int[][]> edgeAssoType3 = new HashMap<Edge, int[][]>();
    public int[] id2KeyId = null;
    public HashMap<Edge, Map<Integer, int[][]>> edgeAssoTypeH = new HashMap<Edge, Map<Integer, int[][]>>(); // edge -> v
    public HashMap<Integer, Set<Edge>> midTypeAssoEdge = new HashMap<Integer, Set<Edge>>(); // v -> edges
    public HashMap<Integer, int[][]> midAssoType3 = new HashMap<Integer, int[][]>(); // v-> (p,t)
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    public Map<Integer, Integer> indexK = new HashMap<Integer, Integer>();
    public List<double[]> cornerBound = new ArrayList<>();
    long findOtherTypeTime = 0;
    long findThisTypeTime = 0;
    long mergeIntervalTime = 0;
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

    public AdvancedHType(int graph[][], int vertexType[], int edgeType[], double weight[],
                         int queryK, MetaPath queryMPath) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
        this.weight = weight;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        this.sortedIndex = sort.sortedIndex(weight);
        for (double value : weight) {
            sortedWeight.add(value);
        }
    }

    public Map<double[], Set<Integer>> computeComm(String datasetName) throws ExecutionException, InterruptedException {

        FastBCore BKCore = new FastBCore(graph, vertexType, edgeType);
        Set<Set<Integer>> conSet = BKCore.query(queryMPath, queryK);
        Set<Integer> keepSet = new HashSet<Integer>(); // contain all type nodes. (eg : authors + papers)
        for (Set<Integer> subSet : conSet) {
            keepSet.addAll(subSet);
        }
        System.out.println(conSet.size());
        System.out.println(keepSet.size());
        HomoGraphBuilderImprved GP = new HomoGraphBuilderImprved(graph, vertexType, edgeType, queryMPath, weight);
        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        int minVer = 0;

        for (Set<Integer> type1 : conSet) {
            Set<Integer> deleteSet = new HashSet<>();

            System.out.println("New Connected Component size is :  " + type1.size());
            long sTime = System.currentTimeMillis();
//            BatchSearchImproved association = GP.build(type1);
            BatchSearchImproved association = GP.buildWithDeleteSet(type1, deleteSet);

            long eTime = System.currentTimeMillis();
            System.out.println("build time is : " + (eTime - sTime) + ":ms");

            pnbMap = association.pnbMap;
            edgeAssoType3 = association.edgeAssoType3;
            edgeAssoTypeH = association.edgeAssoTypeH;
            midTypeAssoEdge = association.midTypeAssoEdge;
            midAssoType3 = association.midAssoType3;
//            Set<Integer> midType = association.typeList.get(2);
            Set<Integer> midType = association.midTypeAssoEdge.keySet();
            List<Integer> sortedType2 = new ArrayList<Integer>();
            List<Integer> sortedType3 = new ArrayList<Integer>();
            List<Integer> sortedMid = new ArrayList<Integer>();
            long startSort = System.currentTimeMillis();
//            pnb2WeightHeap = sorted4WeightHeap(pnbMap);
            for (Integer key : pnbMap.keySet()) {
                GraphSize += pnbMap.get(key).size();
            }
            long endSort = System.currentTimeMillis();
            System.out.println("sort time is : " + (endSort - startSort) + ":ms");
            for (Integer v : sortedIndex) {
                if (vertexType[v] == queryMPath.getEndType() && type1.contains(v)) {
                    sortedType1.add(v);
                }
                if (midType.contains(v)) {
                    sortedMid.add(v);
                }
            }
            tmpMap = deepCopy.copyMap(pnbMap);
            ArrayList<Integer> cvs = new ArrayList<Integer>();
            Deque<Integer> keys = new ArrayDeque<Integer>();
            ArrayList<Pair<Integer, Double[]>> conditionInf = new ArrayList<Pair<Integer, Double[]>>();
            long startTime = System.currentTimeMillis();
            createEdgeAsso(-1);
            while (!sortedType1.isEmpty()) {
                if (sortedMid.isEmpty()) {
                    break;
                }
                minVer = sortedMid.iterator().next();
                System.out.println("f4-weighted is :" + weight[minVer] + "| left size is: " + sortedMid.size());
                GetHofHPlus getHofHPlus = new GetHofHPlus(weight, queryK, sortedType1, sortedType2, sortedType3, weight[minVer], tmpMap, edgeAssoType3);
                result.putAll(getHofHPlus.computeComm());
                while(!sortedMid.isEmpty() && weight[sortedMid.iterator().next()] == weight[minVer]) {

                    minVer = sortedMid.iterator().next();
                    DeleteVer(minVer, sortedMid);
                }
//                association.midAssoType3
                long endTime = System.currentTimeMillis();
//                System.out.println("finish this f2 compute time is : " + (endTime - startTime) + "ms");
            }
        }

        System.out.println("find this time is : " + findThisTypeTime);
        System.out.println("find other time is : " + findOtherTypeTime);
        System.out.println("merge internal  time is : " + mergeIntervalTime);
        System.out.println("all visited graph size is  : " + vistedMaps);
        String logInfo = "Advanced3" + "\t" + datasetName + "\t" + queryMPath.toString() + "\t" + queryK + "\t" + "visted size is : " + vistedMaps + "\t" + "Original Graph is : " + GraphSize;
        System.out.println(logInfo);
        LogAna3.log(logInfo);
        result = analysizeResult(result);
        return result;
    }

    private void DeleteVer(int minVer, List<Integer> sortedMid) {
        sortedMid.remove(sortedMid.indexOf(minVer));
        double f3 = weight[minVer];
        createEdgeAsso(f3);
        Set<Edge> endptsSet = midTypeAssoEdge.get(minVer);

        for (Edge endpts : endptsSet) {
            int start = endpts.getKey()[0];
            int end = endpts.getKey()[1];
            int count = edgeAssoTypeH.get(endpts).size();
            count = count - 1;
            if (count == 0) {
                Set<Integer> nb = tmpMap.get(start);
                if (tmpMap.containsKey(start)) {
                    nb.remove(end);
                    if (nb.size() < queryK && sortedType1.contains(start)) {
                        DeleVerTargetType(start, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            tmpMap.replace(start, nb);
                        }
                    }
                }
                if (tmpMap.containsKey(end)) {
                    nb = tmpMap.get(end);
                    nb.remove(start);
                    if (nb.size() < queryK && sortedType1.contains(end)) {
                        DeleVerTargetType(end, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            tmpMap.replace(end, nb);
                        }
                    }
                }
            }
            edgeAssoTypeH.get(endpts).remove(minVer);
        }
    }

    class PairCompare implements Comparator<int[]> {
        private double[] weight;

        public PairCompare(double[] weight) {
            this.weight = weight;
        }

        @Override
        public int compare(int[] left, int[] right) {
            if (weight[left[0]] != weight[right[0]]) {
                return weight[left[0]] < weight[right[0]] ? -1 : 1;
            } else {
                return weight[left[1]] < weight[right[1]] ? -1 : 1;

            }
        }

    }

    public Set<int[]> filterInt(List<int[]> alivePair) {
        alivePair.sort(new PairCompare(weight));
        Set<int[]> validPair = new HashSet<>();
        int length = alivePair.size() - 1;
        validPair.add(alivePair.get(length));
        double type2Limit = weight[alivePair.get(length)[1]];
        for (int i = length - 1; i >= 0; i--) {
            int[] curPair = alivePair.get(i);
            if (weight[curPair[1]] <= type2Limit) {
                continue;
            }
            type2Limit = weight[curPair[1]];
            validPair.add(curPair);
        }
        return validPair;
    }

    private void createEdgeAsso(double f3) {
        edgeAssoType3.clear();
        Set<Edge> deleteSets = new HashSet<>();

        for (Integer start : tmpMap.keySet()) {
            for (Integer end : tmpMap.get(start)) {
                if (start < end) {
                    Edge edge = new Edge(start, end);
                    Map<Integer, int[][]> tmpAssoType3 = edgeAssoTypeH.get(edge);
                    List<int[]> alivePair = new LinkedList<>();
                    for (Integer midVertex : tmpAssoType3.keySet()) {
                        if (weight[midVertex] > f3) {
                            alivePair.addAll(Arrays.asList(tmpAssoType3.get(midVertex)));
                        }
                    }
                    if (!alivePair.isEmpty()) {
                        Set<int[]> aliveSet = filterInt(alivePair);
                        int[][] aliveArray = new int[aliveSet.size()][2];
                        int cnt = 0;
                        for (int[] pair : aliveSet) {
                            aliveArray[cnt][0] = pair[0];
                            aliveArray[cnt][1] = pair[1];
                            cnt += 1;
                        }
                        edgeAssoType3.put(edge, aliveArray);
                    } else {
                        deleteSets.add(edge);
                    }
                }
            }
        }

        if (!deleteSets.isEmpty()) {
            for (Edge endpts : deleteSets) {
                int start = endpts.getKey()[0];
                int end = endpts.getKey()[1];

                Set<Integer> nb = tmpMap.get(start);
                if (tmpMap.containsKey(start)) {
                    nb.remove(end);
                    if (nb.size() < queryK && sortedType1.contains(start)) {
                        DeleVerTargetType(start, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            tmpMap.replace(start, nb);
                        }
                    }
                }
                if (tmpMap.containsKey(end)) {
                    nb = tmpMap.get(end);
                    nb.remove(start);
                    if (nb.size() < queryK && sortedType1.contains(end)) {
                        DeleVerTargetType(end, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            tmpMap.replace(end, nb);
                        }
                    }
                }
            }
        }

    }

    public void DeleVerTargetType(int v, Set<Integer> vertices) {
        if (vertices.isEmpty()) {
            return;
        }
        if (vertices.contains(v)) {
            vertices.remove(v);
        } else {
            return;
        }

        Set<Integer> pnbSet = tmpMap.get(v);

        for (int pnb : pnbSet) {
            if (sortedType1.contains(pnb)) {
                Set<Integer> tmpSet = tmpMap.get(pnb);
                tmpSet.remove(v);
                if (tmpSet.size() < queryK) {
                    DeleVerTargetType(pnb, sortedType1);
                } else {
                    tmpMap.replace(pnb, tmpSet);
                }
            }
        }
        tmpMap.remove(v);
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
//        String metapath = "47-83-422-746-51-83-422-746-47";
//        String metapath = "2-4-0-0-1-3-0-2-3-5-0-0-1-3-0-1-2";
//        String metapath = "2-4-0-0-1-3-0-2-3-5-0-0-1-3-0-1-2";
        String metapath = "87-870-189-7-55-81-2-744-55-670-189-207-87";
//        String metapath = "87-870-189-7-55-81-0-744-55-670-189-207-87";
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

        AdvancedHType InfCommunities = new AdvancedHType(graph, vertexType, edgeType, weight, 5, metaPath1);
        Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
        Communities.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });
        long endTime = System.currentTimeMillis();
        System.out.println("总结果个数为  cnt : " + Communities.size());
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}

