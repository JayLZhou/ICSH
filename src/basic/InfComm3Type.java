package basic;

import build.BatchSearch;
import build.Edge;
import build.FastBCore;
import build.HomoGraphBuilder;
import javafx.util.Pair;
import util.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class InfComm3Type implements Comm3Type {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    Map<Integer, Set<Edge>> FTAndFT = new HashMap<Integer, Set<Edge>>();
    Set<Edge> weightChange = new HashSet<Edge>();
    public HashMap<Edge, int[][]> edgeAssoComm3 = new HashMap<Edge, int[][]>();

    Sort sort = new Sort();
    deepCopy deepCopy = new deepCopy();
    int res_cnt = 0;
    private ArrayList<Double> sortedWeight = new ArrayList<>();
    long visitedMaps = 0;
    long GraphSize = 0;

    public InfComm3Type(int graph[][], int vertexType[], int edgeType[], double weight[],
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
        // p 对应的 a : t
        for (Edge endpts : endptsSet) {
            if (!edgeAssoComm3.containsKey(endpts)) {
                continue;
            }
            int[][] pairSet = edgeAssoComm3.get(endpts);
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

                edgeAssoComm3.remove(endpts);
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
                edgeAssoComm3.replace(endpts, newArr);
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
            edgeAssoComm3.remove(endpts);
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

    public void analysizeResult(Map<double[], Set<Integer>> Communities) {
        List<double[]> cornerBound = new ArrayList<>();

//         = InfCommunities.computeComm("");
        Map<Double, List<double[]>> checkResult = new HashMap<>();
        Set<Double> type1Set = new HashSet<>();
        for (double[] inf : Communities.keySet()) {
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
        Map<Double, List<double[]>> infMap = new HashMap<>();
        for (double key : checkResult.keySet()) {
            Set<double[]> add = filter(checkResult.get(key));
            List<double[]> addList = new ArrayList<>();
            addList.addAll(add);
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
                }
            }
            cornerBound = updateCorner(cornerBound, infMap.get(type1List.get(i)), weight);
        }
        for (double[] inf : validKey) {
            System.out.println(Arrays.toString(inf));
        }

        System.out.println("总结果个数为 🎉 cnt : " + cnt);
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
        HomoGraphBuilder GP = new HomoGraphBuilder(graph, vertexType, edgeType, queryMPath, weight);
        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        int minVer = 0;
        for (Set<Integer> type1 : conSet) {

            System.out.println("New Connected Component size is :  " + type1.size());
            long sTime = System.currentTimeMillis();
            BatchSearch association = GP.build(type1);
            long eTime = System.currentTimeMillis();
            System.out.println("build time is : " + (eTime - sTime));
            pnbMap = association.pnbMap;
            for (Integer key : pnbMap.keySet()) {
                GraphSize += pnbMap.get(key).size();
            }
            this.FTAndFT = association.FTAndFT;
            edgeAssoComm3 = association.edgeAssoComm3;
            Set<Integer> type3 = association.type3;
            Set<Integer> type2 = association.type2;
            Set<Integer> sortedType1 = new LinkedHashSet<Integer>();
            List<Integer> sortedType2 = new ArrayList<Integer>();
            List<Integer> sortedType3 = new ArrayList<Integer>();
            System.out.println("type3 size is : " + association.type3.size());
            System.out.println("type2 size is : " + association.type2.size());

            Set<Double> sortedType3WeightSet = new LinkedHashSet<>();
            for (Integer v : sortedIndex) {
                if (vertexType[v] == queryMPath.getEndType() && type1.contains(v)) {
                    sortedType1.add(v);
                }
                if (vertexType[v] == queryMPath.getSecondType() && type2.contains(v)) {
                    sortedType2.add(v);
                }
                if (vertexType[v] == queryMPath.getThirdType() && type3.contains(v)) {
                    sortedType3.add(v);
                }
            }
            for (Integer i : sortedType2) {
                sortedType3WeightSet.add(weight[i]);
            }
            List<Double> sortedType3Weight = new ArrayList<>(sortedType3WeightSet);
//            int[] c = {81873, 81941, 82757, 82980, 83608, 82206};
//            double minf1 = 10000000.0;
//            for (int a : c) {
//                minf1 = Double.min(weight[a], minf1);
//            }
//            System.out.println("f1 : " + minf1);
//            for (int start : c) {
//                for (int end : c) {
//                    if (start == end) {
//                        continue;
//                    }
//                    int type = 2;
//                    double value = -1;
//                    int keyId = -1;
//                    Edge edge = new Edge(new int[]{start, end});
//                    for (int[] key : edgeAssoComm3.get(edge)) {
//                        if (weight[key[type - 2]] > value) {
//                            keyId = key[type - 2];
//                            value = weight[keyId];
//                        }
//                    }
//                    double othertype3 = 0.0;
//                    for (int[] key : edgeAssoComm3.get(edge)) {
//                        if (weight[key[0]] >= value) {
//                            othertype3 = Double.max(weight[key[3 - 2]], othertype3);
//                        }
//                    }
//                    System.out.println("edge :" + new int[]{start, end} + "|" + value + "|" + othertype3);
//                }
//            }
            while (!sortedType1.isEmpty()) {
                long startTime = System.currentTimeMillis();
                minVer = sortedType3.iterator().next();
                System.out.println("f3 weight is : " + weight[minVer]);

                Get2Of3 Getf2f3 = new Get2Of3(edgeAssoComm3, sortedWeight, queryK, weight[minVer], pnbMap, sortedType1, weight);
                result.putAll(Getf2f3.computeComm());
                for (Integer key : pnbMap.keySet()) {
                    visitedMaps += pnbMap.get(key).size();
                }
//                }
                DeleVer(minVer, sortedType3, sortedType1);
                long endTime = System.currentTimeMillis();
                System.out.println("finish this f2 compute time is : " + (endTime - startTime) + "ms");
//                System.out.println("visted graph size is : " + visitedMaps);
            }

        }
//        String logInfo = "Basic Type 3 : " + "\t" + datasetName + "\t" + queryMPath.toString() + "\t" + queryK + "\t" + "visted:" + visitedMaps + "\t" + "Original Graph is : " + GraphSize;
//        LogAna3.log(logInfo);
//        System.out.println(result);
        analysizeResult(result);
        return result;

    }


    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
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
//        int vertex1[] = {1, 0, 3, 0, 1}, edge1[] = {3, 2, 5, 0};
//        int vertex1[] = {3, 0, 1, 0, 3}, edge1[] = {5, 0, 3, 2}; // S-G-D-G-S
//        int vertex1[] = {3, 0, 2, 0, 3}, edge1[] = {5, 1, 4, 2}; // S-G-C-G-S

//        int vertex1[] = {1, 0, 2, 0, 1}, edge1[] = {1, 2, 3, 0}; // A-M-D-M-A for imdb
//        int vertex1[] = {2, 0, 1, 0, 2}, edge1[] = {3, 0, 1, 2}; // D-M-A-M-D for imdb


        String metaPath = "87-870-189-7-55-670-189-207-87";
        MetaPath metaPath1 = new MetaPath(metaPath);
//        DataReader dataReader = new DataReader(Config.musicGraph, Config.musicVertex, Config.musicEdge, Config.musicWeight);
//        DataReader dataReader = new DataReader(Config.tmdbGraph, Config.tmdbVertex, Config.tmdbEdge, Config.tmdbWeight);
//        DataReader dataReader = new DataReader(Config.IMDBGraph, Config.IMDBVertex, Config.IMDBEdge, Config.IMDBWeight);
        DataReader dataReader = new DataReader(Config.dbpediaGraph, Config.dbpediaVertex, Config.dbpediaEdge, Config.dblpWeight);
//        DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
//        DataReader dataReader = new DataReader(Config.pubmedGraph, Config.pubmedVertex, Config.pubmedEdge, Config.pubmedWeight);
//        DataReader dataReader = new DataReader(Config.IMDBGraph, Config.IMDBVertex, Config.IMDBEdge, Config.IMDBWeight);

        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();
        List<double[]> cornerBound = new ArrayList<>();

        InfComm3Type InfCommunities = new InfComm3Type(graph, vertexType, edgeType, weight, 5, metaPath1);
        Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
        Map<Double, List<double[]>> checkResult = new HashMap<>();
        Set<Double> type1Set = new HashSet<>();
        for (double[] inf : Communities.keySet()) {
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
        Map<Double, List<double[]>> infMap = new HashMap<>();
        for (double key : checkResult.keySet()) {
            Set<double[]> add = filter(checkResult.get(key));
            List<double[]> addList = new ArrayList<>();
            addList.addAll(add);
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
                }
            }
            cornerBound = updateCorner(cornerBound, infMap.get(type1List.get(i)), weight);
        }
        for (double[] inf : validKey) {
            System.out.println(Arrays.toString(inf));
        }

        long endTime = System.currentTimeMillis();
        System.out.println("总结果个数为 🎉 cnt : " + cnt);

        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
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
}
