package advanced;
import basic.Comm2Type;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import build.BatchSearch;
import build.FastBCore;
import build.HomoGraphBuilder;
import javafx.util.Pair;
import util.*;

import java.util.*;

public class Basic2Plus implements Comm2Type {
    private static DataReader dataReader;
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private double constraints[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    private long visitededges = 0;

    public Basic2Plus(int graph[][], int vertexType[], int edgeType[], double weight[],
                        double constraints[], int queryK, MetaPath queryMPath) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
        this.weight = weight;
        this.constraints = constraints;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        Sort sort = new Sort();
        this.sortedIndex = sort.sortedIndex(weight);
    }

    // Compute com
    public Map<double[], Set<Integer>> computeComm(String datasetName) {
        FastBCore BKCore = new FastBCore(graph, vertexType, edgeType);
        Set<Set<Integer>> conSet = BKCore.query(queryMPath, queryK);
        Set<Integer> keepSet = new HashSet<Integer>();
        for (Set<Integer> subSet : conSet) {
            keepSet.addAll(subSet);
        }
        System.out.println(conSet.size());
        System.out.println(keepSet.size());
        HomoGraphBuilder GP = new HomoGraphBuilder(graph, vertexType, edgeType, queryMPath, weight);

        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        double f1_last = Double.MAX_VALUE;
        double f2_min = Double.MAX_VALUE;
        boolean first = true;
        for (Set<Integer> type1 : conSet) {
            BatchSearch association = GP.build(type1);
            Map<Integer, Set<Integer>> pnbMap = association.pnbMap;
            Set<Integer> type2 = association.verAsso.keySet();


            List<Integer> sortedType1 = new ArrayList<Integer>();
            List<Integer> sortedType2 = new ArrayList<Integer>();
            for (Integer v : sortedIndex) {
                if (vertexType[v] == queryMPath.getEndType() && type1.contains(v)) {
                    sortedType1.add(v);
                }
                if (vertexType[v] == queryMPath.getSecondType() && type2.contains(v)) {
                    sortedType2.add(v);
                }
            }
            TypeBinary typeMax = new TypeBinary(vertexType, weight, queryK, queryMPath,
                    pnbMap, association, sortedType1, sortedType2);

            double f1 = Double.MIN_VALUE;
            double f2 = Double.MIN_VALUE;
            double tempConst[] = {-1, -1};
            for (int i = 0; i < constraints.length; i++) {
                tempConst[i] = constraints[i];
            }

            while (f2 >= 0) {
                Set<Integer> community = new HashSet<Integer>();
                f2 = typeMax.getTypeMax(tempConst, queryMPath.getSecondType(), community);

                System.out.println("f2 = " + f2);
                if (f2 == -1) {
                    break;
                }
                for (int i = 0; i < constraints.length; i++) {
                    tempConst[i] = constraints[i];
                }

                tempConst[1] = f2;
                f1 = typeMax.getTypeMax(tempConst, queryMPath.getEndType(), community);
                for (int i = 0; i < constraints.length; i++) {
                    tempConst[i] = constraints[i];
                }
                if (f1 == -1) {
                    break;
                }
                tempConst[0] = f1;

                double influence[] = {f1, f2};
                System.out.println("| f1 = " + Double.toString(influence[0]) + " | f2 = " + Double.toString(influence[1]));
                result.put(influence, community);
                f2_min = Math.min(f2_min, influence[1]);
            }
        }
        result = filterComm(result);
        System.out.println("The result size is  : 🎉" + result.size());
        return result;
    }
    private Map<double[], Set<Integer>> filterComm(Map<double[], Set<Integer>> Communities) {
        Map<double[], Set<Integer>> res = new HashMap<>();

        List<Double> type1List = new ArrayList<>();
        Map<Double, Double> f12f2 = new HashMap<>();
        Map<Pair<Double, Double>, Set<Integer>> mapComm = new HashMap<>();
        for (Map.Entry<double[], Set<Integer>> entries : Communities.entrySet()) {
            type1List.add(entries.getKey()[0]);
            // fix : big bug
            if (f12f2.containsKey(entries.getKey()[0])) {
                double tmp = entries.getKey()[1];
                if (f12f2.get(entries.getKey()[0]) < tmp) {
                    f12f2.replace(entries.getKey()[0], entries.getKey()[1]);
                }
            } else {
                f12f2.put(entries.getKey()[0], entries.getKey()[1]);
            }
            mapComm.put(new Pair<>(entries.getKey()[0], entries.getKey()[1]), entries.getValue());
        }
        int cnt = 0;
        if (type1List.size() <= 1) {
            return Communities;
        }
        type1List.sort(new myComparator(weight));
        Double f_1 = type1List.get(type1List.size() - 1);
        Double f_2 = f12f2.get(f_1);
        res.put(new double[]{f_1, f_2}, mapComm.get(new Pair<>(f_1, f_2)));
        for (int i = type1List.size() - 2; i >= 0; i--) {
            Double new_f1 = type1List.get(i);

            Double new_f2 = f12f2.get(new_f1);
            if (new_f1 == 8.0) {
                System.out.println("new f_2 is :" + new_f2);
            }
//        if (new)
            if (new_f2 > f_2) {
                f_2 = new_f2;
                res.put(new double[]{new_f1, new_f2}, mapComm.get(new Pair<>(new_f1, new_f2)));
            }
        }

        return res;
    }
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//        int vertex1[] = {1, 0, 1}, edge1[] = {1, 0};
//        int vertex1[] = {3, 0, 3}, edge1[] = {4, 5};

//        int vertex1[] = {0, 2, 0}, edge1[] = {1, 4};
//        int vertex1[] = {1, 0, 1}, edge1[] = {1, 0};
//        int vertex1[] = {3, 0, 3}, edge1[] = {5, 2};
        int vertex1[] = {6, 0, 6}, edge1[] = {11, 10}; // tmdb
//        0-1-2-4-0
        DataReader dataReader = new DataReader(Config.tmdbGraph, Config.tmdbVertex, Config.tmdbEdge, Config.tmdbWeight);
//        DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);


        MetaPath metaPath1 = new MetaPath(vertex1, edge1);
        double constraints[] = {-1, -1};

        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();

        Basic2Plus InfCommunities = new Basic2Plus(graph, vertexType, edgeType,
                weight, constraints, 5, metaPath1);
        Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
        BatchLinker test = new BatchLinker(graph, vertexType, edgeType);
        for (double[] key : Communities.keySet()) {
            Set<Integer> value = Communities.get(key);
            List<Integer> vertices = new ArrayList<Integer>(value);
            Set<Integer> connectedSet = test.link(vertices.get(0), metaPath1);
            for (int v : value) {
                if (!connectedSet.contains(v)) {
                    Communities.remove(key);
                }
            }
        }

        Communities.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}