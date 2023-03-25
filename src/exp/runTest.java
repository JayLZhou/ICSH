package exp;
// This class is provided a interface to run the all algorithms proposed in our paper
// For h=2, {Basic2D, BasicHalf2D, Fast2D}
// For h=3, {Basic3D, Fast3D}
// For h=4, {Basic4D, Fast4D}
import advanced.Advanced2Type;
import advanced.Advanced3Type;
import advanced.AdvancedHType;
import advanced.Basic2Plus;
import basic.*;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class runTest {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private MetaPath metaPath;
    private List<MetaPath> queryMPathList;
    private InfComm2Type infComm2Type;
    private Advanced2Type advanced2Type;
    private Basic2Plus basic2Plus;
    private InfComm3Type infComm3Type;
    private Advanced3Type advanced3Type;
    private BasicHType basicHType;
    private AdvancedHType advancedHType;

    public runTest(String graphDataSetPath, MetaPath metaPath,  String dataSetName) {
        dataReader = new DataReader(graphDataSetPath + "/graph.txt", graphDataSetPath + "/vertex.txt", graphDataSetPath + "/edge.txt", graphDataSetPath + "/weight.txt");
        graph = dataReader.readGraph();
        vertexType = dataReader.readVertexType();
        edgeType = dataReader.readEdgeType();
        weight = dataReader.readWeight();
        this.dataSetName = dataSetName;
        this.queryMPathList = new ArrayList<>();
        this.metaPath = metaPath;

    }

    private void initComm2(int queryK, MetaPath metaPath) {
        infComm2Type = new InfComm2Type(graph, vertexType, edgeType, weight, new double[]{-1, -1}, queryK, metaPath);
        advanced2Type = new Advanced2Type(graph, vertexType, edgeType, weight, queryK, metaPath);
        basic2Plus = new Basic2Plus(graph, vertexType, edgeType, weight, new double[]{-1, -1}, queryK, metaPath);
    }

    private void initComm3(int queryK, MetaPath metaPath) {
        infComm3Type = new InfComm3Type(graph, vertexType, edgeType, weight, queryK, metaPath);
        advanced3Type = new Advanced3Type(graph, vertexType, edgeType, weight, queryK, metaPath);
    }

    private void initComm4(int queryK, MetaPath metaPath) {
        basicHType = new BasicHType(graph, vertexType, edgeType, weight, queryK, metaPath);
        advancedHType = new AdvancedHType(graph, vertexType, edgeType, weight, queryK, metaPath);
    }
    public void testType2(Comm2Type comm2Type, String algorithmInfo, String metaPathName, String queryK) throws ExecutionException, InterruptedException {
        Map<double[], Set<Integer>> setMap = comm2Type.computeComm(dataSetName);
        if (setMap.isEmpty()) {
            return;
        }
        System.out.println("This is the result for the algorithm: " + algorithmInfo + " queryK: " + queryK + " Meta-path is: " + metaPathName);
        setMap.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });

    }
    public void testType3(Comm3Type comm3Type, String algorithmInfo, String metaPathName, String queryK) throws ExecutionException, InterruptedException {
        Map<double[], Set<Integer>> setMap = comm3Type.computeComm(dataSetName);
        if (setMap.isEmpty()) {
            return;
        }
        System.out.println("This is the result for the algorithm: " + algorithmInfo + " queryK: " + queryK + " Meta-path is: " + metaPathName);
        setMap.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });

    }
    public void testType4(Comm3Type comm3Type , String algorithmInfo, String metaPathName, String queryK) throws ExecutionException, InterruptedException {
        Map<double[], Set<Integer>> setMap = comm3Type.computeComm(dataSetName);
        if (setMap.isEmpty()) {
            return;
        }
        System.out.println("This is the result for the algorithm: " + algorithmInfo + " queryK: " + queryK + " Meta-path is: " + metaPathName);
        setMap.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });

    }
    public void test(int queryK, String method) throws ExecutionException, InterruptedException {

            if (Objects.equals(method, "Fast2D") || Objects.equals(method, "Basic2D") ||  Objects.equals(method, "BasicHalf2D")) {
                initComm2(queryK, metaPath);
                switch (method) {
                    case "BasicHalf2D": {
                        testType2(basic2Plus, "BasicHalf2D", metaPath.toString(), Integer.toString(queryK));
                        break;
                    }
                    case "Fast2D": {
                        testType2(advanced2Type, "Fast2D", metaPath.toString(), Integer.toString(queryK));
                        break;
                    }
                    case "Basic2D": {
                        testType2(infComm2Type, "Basic2D", metaPath.toString(), Integer.toString(queryK));
                        break;
                    }
                    default:
                        System.out.println("There is no other type2 algorithms");
                        break;
                }
            }

        if (Objects.equals(method, "Fast3D") || Objects.equals(method, "Basic3D")) {
            initComm3(queryK, metaPath);
            switch (method) {
                case "Basic3D": {
                    testType3(infComm3Type, "Basic3D", metaPath.toString(), Integer.toString(queryK));
                    break;
                }
                case "Fast3D": {
                    testType3(advanced3Type, "Fast3D", metaPath.toString(), Integer.toString(queryK));
                    break;
                }
                default:
                    System.out.println("There is no other type3 algorithms");
                    break;
            }
        }

        if (Objects.equals(method, "Fast4D") || Objects.equals(method, "Basic4D")) {
            initComm4(queryK, metaPath);
            switch (method) {
                case "Basic4D": {
                    testType4(basicHType, "Basic4D", metaPath.toString(), Integer.toString(queryK));
                    break;
                }
                case "Fast4D": {
                    testType4(advancedHType, "Fast4D", metaPath.toString(), Integer.toString(queryK));
                    break;
                }
                default:
                    System.out.println("There is no other type4 algorithms");
                    break;
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // You need to specify some parameters:
        // Dataset: TMDB
        // queryK: e.g., 5
        // Meta-Path: e.g.,Movie-Director-Movie
        // Method: Fast2D
        int queryK  = 5;
        String metaPath_str = "0-6-4-7-0";
        MetaPath metaPath = new MetaPath(metaPath_str);
        String dataSetName = "tmdb";
        String method = "Fast2D";
        String graphDataSetPath = Config.root + "/" + dataSetName;

        runTest vdTest = new runTest(graphDataSetPath, metaPath, dataSetName);
        vdTest.test(queryK, method);

    }
}
