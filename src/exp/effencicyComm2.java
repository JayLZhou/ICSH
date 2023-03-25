package exp;

import advanced.Advanced2Type;
import advanced.Basic2Plus;
import basic.Comm2Type;
import basic.InfComm2Type;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class effencicyComm2 {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private InfComm2Type infComm2Type;
    private Advanced2Type advanced2Type;
    private Basic2Plus basic2Plus;


    public effencicyComm2(String graphDataSetPath, String metaPathsPath, String dataSetName) {
        dataReader = new DataReader(graphDataSetPath + "/graph.txt", graphDataSetPath + "/vertex.txt", graphDataSetPath + "/edge.txt", graphDataSetPath + "/weight.txt");
        graph = dataReader.readGraph();
        vertexType = dataReader.readVertexType();
        edgeType = dataReader.readEdgeType();
        weight = dataReader.readWeight();
        this.dataSetName = dataSetName;
        this.queryMPathList = new ArrayList<>();
        getQueryMetaPathSet(metaPathsPath);
    }

    private void initComm2(int queryK, MetaPath metaPath) {
        infComm2Type = new InfComm2Type(graph, vertexType, edgeType, weight, new double[]{-1, -1}, queryK, metaPath);
        advanced2Type = new Advanced2Type(graph, vertexType, edgeType, weight, queryK, metaPath);
        basic2Plus = new Basic2Plus(graph, vertexType, edgeType, weight, new double[]{-1, -1}, queryK, metaPath);
    }

    private void getQueryMetaPathSet(String metaPathsPath) {
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(metaPathsPath + "/meta2Paths.txt"));
            String line = null;
            while ((line = stdin.readLine()) != null) {
                String metaPath = line;
                System.out.println(line);
                MetaPath metaPath1 = new MetaPath(metaPath);
                this.queryMPathList.add(metaPath1);
            }
            stdin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long test(Comm2Type comm2Type, String algorithmInfo, String metaPathName, String queryK) throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        Map<double[], Set<Integer>> setMap = comm2Type.computeComm(dataSetName);
        if (setMap.isEmpty()) {
            return 0;
        }

        long end = System.currentTimeMillis();
        String partInfo = dataSetName + "\t" + algorithmInfo + "\t" + metaPathName + "\t" + (end - start) + "\t" + queryK + "\t" + "result: " + setMap.size();
        LogPart.log(partInfo);
        return (end - start);
    }

    public void testAll(int queryK) throws ExecutionException, InterruptedException {
        long advanceTime = 0;
        long imporveAdvanceTime = 0;
        long basciTime = 0;
        long basciPlusTime = 0;
        int validCnt = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("122-7-13-670-122"));
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
            long bt = test(infComm2Type, "basicType2", metaPath.toString(), Integer.toString(queryK));
            if (bt != 0) {
                basciTime += bt;
                validCnt += 1;
                try {
                    System.gc();
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            long blt = test(basic2Plus, "basicPlusType2", metaPath.toString(), Integer.toString(queryK));
            if (blt != 0) {
                basciPlusTime += blt;
                validCnt += 1;
                try {
                    System.gc();
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

//
            long iat = test(advanced2Type, "ImprovedAdvancedType2", metaPath.toString(), Integer.toString(queryK));
            if (iat != 0) {
                imporveAdvanceTime += iat;
                validCnt += 1;
                try {
                    System.gc();
                    Thread.sleep(10000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        String basicInfo = dataSetName + "\t" + "BasicInfComm2Type" + "\t"
                + "Cost Time is : " + "\t" + String.valueOf(basciTime / validCnt) + " ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(basicInfo, 2);
        String basicPlusInfo = dataSetName + "\t" + "BasicPlusComm2Type" + "\t"
                + "Cost Time is : " + "\t" + String.valueOf(basciPlusTime / validCnt) + " ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(basicPlusInfo, 2);
        String advanceInfo = dataSetName + "\t" + "AdvancedType2" + "\t"
                + "Cost Time is : " + "\t" + String.valueOf(imporveAdvanceTime / validCnt) + " ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(advanceInfo, 2);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("music");
//        dataSetList.add("tmdb");
        dataSetList.add("DBLPWithWeight");
//        dataSetList.add("smallimdb");
//        dataSetList.add("DBPedia");
        int[] kArry = new int[]{11,13,15};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            effencicyComm2 vdTest = new effencicyComm2(graphDataSetPath, metaPathsPath, dataSetName);
            for (int k : kArry) {
                vdTest.testAll(k);
            }
            LogFinal.log("\n", 2);
            LogPart.log("\n");
        }
    }
}
