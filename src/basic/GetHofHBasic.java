package basic;

import build.BatchSearch;
import build.Edge;
import build.FastBCore;
import build.HomoGraphBuilder;
import javafx.util.Pair;
import util.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class GetHofHBasic {

    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    public HashMap<Edge, int[][]> edgeAssoType3 = new HashMap<Edge, int[][]>();
    public int[] id2KeyId = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    double f4 = 0;
    Sort sort = new Sort();
    Map<Integer, Set<Edge>> FTAndFT = new HashMap<Integer, Set<Edge>>();

    deepCopy deepCopy = new deepCopy();
    Map<Integer, List<DualHeapComm3>> pnb2WeightHeap = new HashMap<Integer, List<DualHeapComm3>>();
    public ArrayList<Double> sortedWeight = new ArrayList<>();
    Set<Integer> sortedType1 = new LinkedHashSet<Integer>();
    List<Integer> sortedType3 = new ArrayList<>();


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



    public GetHofHBasic(double[]weight,
            int queryK, Set<Integer> sortedType1, List<Integer> sortedType3, double f4,Map<Integer, Set<Integer>> pnbMap, HashMap<Edge, int[][]> edgeAssoType3, Map<Integer, Set<Edge>> FTAndFT) {

        this.weight = weight;
        this.queryK = queryK;
        this.sortedIndex = sort.sortedIndex(weight);
        this.sortedType1 = deepCopy.copy(sortedType1);
        this.sortedType3 = deepCopy.copy(sortedType3);

        for (double value : weight) {
            sortedWeight.add(value);
        }
        this.f4 = f4;
        this.edgeAssoType3 = edgeAssoType3;
        this.pnbMap = deepCopy.copyMap(pnbMap);
        this.FTAndFT = FTAndFT;
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
//            addList.addAll(checkResult.get(key));
            addList.addAll(filter(checkResult.get(key)));
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
        // care about the key -> {val1, val2, val3} ...
        // we only save the maximum value for each key.
        HashMap<Double, Double> key2Val = new HashMap<>();
        for (double[] allPair : alivePair) {
            if (key2Val.containsKey(allPair[0])) {
                double val = key2Val.get(allPair[0]);
                if (val < allPair[1]) {
                    key2Val.replace(allPair[0], allPair[1]);
                }
            }else {
                key2Val.put(allPair[0], allPair[1]);
            }
        }

        Set<double[]> validPair = new HashSet<>();
        int length = alivePair.size() - 1;
        validPair.add(new double[] {alivePair.get(length)[0], key2Val.get(alivePair.get(length)[0])});
        Set<Double> vistedSet = new HashSet<>();
        double type2Limit = key2Val.get(alivePair.get(length)[0]);
        for (int i = length - 1; i >= 0; i--) {
            double[] curPair = alivePair.get(i);
            if (vistedSet.contains(curPair[0])) {
                continue;
            } else {
                vistedSet.add(curPair[0]);
            }
            double val = key2Val.get(curPair[0]);
            if (val <= type2Limit) {
                continue;
            }
            type2Limit = val;
            double[] newAdd = new double[] {curPair[0], val};
            validPair.add(newAdd);
        }
        return validPair;
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

    public void DeleVer(int v, List<Integer> vertices, Set<Integer> sortedType1) {
        if (vertices.isEmpty()) {
            return;
        }
        if (vertices.contains(v)) {
            vertices.remove(vertices.indexOf(v));
        } else {
            return;
        }
        if (!FTAndFT.containsKey(v)) {
            return;
        }
        Set<Edge> endptsSet = FTAndFT.get(v);
        for (Edge endpts : endptsSet) {
            if (!edgeAssoType3.containsKey(endpts)) {
                continue;
            }
            int[][] pairSet = edgeAssoType3.get(endpts);
            int count = pairSet.length;
            List<int[]> newPair = new ArrayList<>();
            for (int[] pair : pairSet) {
                if (pair[1] != v) {
                    newPair.add(pair);
                }
            }
            int[][] newArr = new int[newPair.size()][2];
            for (int i = 0; i < newArr.length; i++) {
                newArr[i] = newPair.get(i);
            }
            if (count == 1) {
                int start = endpts.getKey()[0];
                int end = endpts.getKey()[1];

                edgeAssoType3.remove(endpts);
                if (pnbMap.containsKey(start)) {
                    Set<Integer> nb = pnbMap.get(start);
                    nb.remove(end);
                    if (nb.size() < queryK && sortedType1.contains(start)) {
                        DeleVer4ST_FT(start, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            pnbMap.replace(start, nb);
                        }
                    }
                }
                if (pnbMap.containsKey(end)) {
                    Set<Integer> nb = pnbMap.get(end);
                    nb.remove(start);
                    if (nb.size() < queryK && sortedType1.contains(end)) {
                        DeleVer4ST_FT(end, sortedType1);
                    } else {
                        if (nb.size() >= queryK) {
                            pnbMap.replace(end, nb);
                        }
                    }
                }
            } else {
                edgeAssoType3.replace(endpts, newArr);
            }
            FTAndFT.remove(v);
        }
    }


    private void DeleVer4ST_FT(int v, Set<Integer> vertices) {
        if (vertices.isEmpty()) {
            return;
        }
        if (vertices.contains(v)) {
            vertices.remove(v);
        } else {
            return;
        }
        Set<Integer> pnbSet = pnbMap.get(v);
        for (int pnb : pnbSet) {
            Edge endpts = new Edge(pnb, v);
            edgeAssoType3.remove(endpts);
            if (vertices.contains(pnb)) {
                Set<Integer> tmpSet = pnbMap.get(pnb);
                tmpSet.remove(v);
                if (tmpSet.size() < queryK) {
                    DeleVer4ST_FT(pnb, vertices);
                } else {
                    pnbMap.replace(pnb, tmpSet);
                }
            }
        }
        pnbMap.remove(v);
    }

    // Baisc4D
    public Map<double[], Set<Integer>> computeComm() throws ExecutionException, InterruptedException {

        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        int minVer = 0;


        while (!sortedType1.isEmpty()) {
            minVer = sortedType3.iterator().next();
            Get2Of3 Getf2f3 = new Get2Of3(edgeAssoType3, sortedWeight, queryK, weight[minVer], pnbMap, sortedType1, weight);
            result.putAll(Getf2f3.computeComm());
            DeleVer(minVer, sortedType3, sortedType1);
        }
        result = analysizeResult(result);
        return result;

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


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();

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

