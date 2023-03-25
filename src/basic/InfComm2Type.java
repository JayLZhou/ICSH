package basic;

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
import util.BatchLinker;
import util.Config;
import util.DataReader;
import util.MetaPath;
import util.Sort;

import java.util.*;
// Algorithm: Basic2D
public class InfComm2Type implements Comm2Type {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private double constraints[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;

    public InfComm2Type(int graph[][], int vertexType[], int edgeType[], double weight[],
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
        double f2_min = Double.MAX_VALUE;
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
            TypeMax typeMax = new TypeMax(vertexType, weight, queryK, queryMPath,
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
                tempConst[0] = f1;

                double influence[] = {f1, f2};
                result.put(influence, community);
                f2_min = Math.min(f2_min, influence[1]);
            }
        }
        System.out.println("The size of results is: " + result.size());
        return result;
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//        int vertex1[] = {1, 0, 1}, edge1[] = {1, 0};
//        int vertex1[] = {3, 0, 3}, edge1[] = {4, 5};

        int vertex1[] = {0, 2, 0}, edge1[] = {2, 3};
//        int vertex1[] = {1, 0, 1}, edge1[] = {1, 0};
//        int vertex1[] = {3, 0, 3}, edge1[] = {4, 5};

        DataReader dataReader = new DataReader(Config.musicGraph, Config.musicVertex, Config.musicEdge, Config.musicWeight);


        MetaPath metaPath1 = new MetaPath(vertex1, edge1);
        double constraints[] = {-1, -1};

        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();

        InfComm2Type InfCommunities = new InfComm2Type(graph, vertexType, edgeType,
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
        System.out.println("The total running time is：" + (endTime - startTime) + "ms");
    }
}