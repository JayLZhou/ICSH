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

public class scalabilityTest2 {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private InfComm2Type infComm2Type;
    private Basic2Plus halfComm2Type;
    private Advanced2Type advanced2Type;
    private SmallGraph sGraph;


    public scalabilityTest2(String graphDataSetPath, String metaPathsPath, String dataSetName) {
        dataReader = new DataReader(graphDataSetPath + "/graph.txt", graphDataSetPath + "/vertex.txt", graphDataSetPath + "/edge.txt", graphDataSetPath + "/weight.txt");
        graph = dataReader.readGraph();
        vertexType = dataReader.readVertexType();
        edgeType = dataReader.readEdgeType();
        weight = dataReader.readWeight();
        this.dataSetName = dataSetName;
        this.queryMPathList = new ArrayList<>();
        getQueryMetaPathSet(metaPathsPath);
        sGraph = new SmallGraph(graph, vertexType, edgeType, weight);

    }

    private void initComm2(int queryK, MetaPath metaPath) {
//        infComm2Type = new InfComm2Type(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, new double[]{-1, -1}, queryK, metaPath);
        halfComm2Type = new Basic2Plus(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, new double[]{-1, -1}, queryK, metaPath);
        advanced2Type = new Advanced2Type(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, queryK, metaPath);
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

    public long test(Comm2Type comm2Type, String algorithmInfo, String metaPathName, String queryK, String part) throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        Map<double[], Set<Integer>> setMap = comm2Type.computeComm(dataSetName);
        if (setMap.isEmpty()) {
            return 0;
        }
        long end = System.currentTimeMillis();
        String partInfo = "scalabilityTest: " + part + "\t" + dataSetName + "\t" + algorithmInfo + "\t" + metaPathName + "\t" + queryK + "\t" + (end - start) + "ms";
        LogPart.log(partInfo);
        return (end - start);
    }

    public void testAll(int queryK, int part, int total) throws ExecutionException, InterruptedException {
        sGraph.getSmallGraph(part, total);

        long basciTime = 0;
        long advanceTime = 0;
        long imporveAdvanceTime = 0;
        int validCnt = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("0-0-1-1-0"));
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
            System.out.println("meta Path is : " + metaPath.toString());
//
          long testTime = test(halfComm2Type, "Basic2Plus", metaPath.toString(), Integer.toString(queryK), Integer.toString(part));
            if (testTime != 0) {
                basciTime += testTime;
                validCnt += 1;
            }
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            long advaTime = test(advanced2Type, "AdvancedType2", metaPath.toString(), Integer.toString(queryK), Integer.toString(part));
            if (advaTime != 0) {
                advanceTime += advaTime;
                validCnt += 1;
            }
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String basicInfo = "Sca: " + part + "\t" + dataSetName + "\t " + " BasicHalf_Plus_Comm2Type " + "\t "
                + String.valueOf(basciTime / validCnt) + "ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(basicInfo, 2);

        String advanceInfo = "Sca: " + part + "\t" + dataSetName + "\t" + "AdvancedType2" + "\t"
                + String.valueOf(advanceTime / validCnt) + "ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(advanceInfo, 2);
//

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<String> dataSetList = new ArrayList<String>();
        dataSetList.add("tmdb");
        dataSetList.add("DBLPWithWeight");
        dataSetList.add("smallimdb");
        dataSetList.add("DBPedia");/**/

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            scalabilityTest2 vdTest = new scalabilityTest2(graphDataSetPath, metaPathsPath, dataSetName);
            for (int i = 1; i <= 4; i++) {
                vdTest.testAll(5, i, 5);
            }
            LogFinal.log("\n", 2);
            LogPart.log("\n");
        }
    }
}
