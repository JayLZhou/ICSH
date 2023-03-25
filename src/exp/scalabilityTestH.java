package exp;

import advanced.Advanced3Type;
import advanced.AdvancedHType;
import basic.BasicHType;
import basic.Comm3Type;
import basic.InfComm3Type;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class scalabilityTestH {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
//    private InfComm3Type infComm3Type;
//    private Advanced3Type advanced3Type;
    private AdvancedHType advancedHType;
    private BasicHType basicHType;
    private SmallGraph sGraph;


    public scalabilityTestH(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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

    private void initComm3(int queryK, MetaPath metaPath) {
//        infComm3Type = new InfComm3Type(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, queryK, metaPath);
//        advanced3Type = new Advanced3Type(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, queryK, metaPath);
        advancedHType = new AdvancedHType(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, queryK, metaPath);
        basicHType = new BasicHType(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType, sGraph.smallGraphWeight, queryK, metaPath);
    }

    private void getQueryMetaPathSet(String metaPathsPath) {
        try {
            BufferedReader stdin = new BufferedReader(new FileReader(metaPathsPath + "/metaHpaths.txt"));
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

    public long test(int part, Comm3Type comm3Type, String algorithmInfo, String metaPathName, String queryk) throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        comm3Type.computeComm(dataSetName);
        long end = System.currentTimeMillis();
        String partInfo = "Sca: " + part + "\t" + dataSetName + "\t" + algorithmInfo + "\t" + metaPathName + "\t" + (end - start) + "ms" + "\t" + queryk + "\t" + "TypeH";
        LogPart.log(partInfo);
        return (end - start);
    }

    public void testAll(int queryK, int part, int total) throws ExecutionException, InterruptedException {
        sGraph.getSmallGraph(part, total);
        long basciTime = 0;
        long advanceTime = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("2-3-0-0-1-1-0-2-2"));
//        queryMPathList.add(new MetaPath("2-4-0-0-1-3-0-2-3-5-0-0-1-3-0-1-2"));
        for (MetaPath metaPath : queryMPathList) {
            initComm3(queryK, metaPath);

            basciTime += test(part, basicHType, "BasicTypeH", metaPath.toString(),Integer.toString(queryK) );
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            advanceTime += test(part, advancedHType, "AdvancedTypeH", metaPath.toString(), Integer.toString(queryK));
            try {
                basicHType = null;
                advancedHType = null;
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
        String basicInfo = "Sca:" + part + "\t" + dataSetName + "\t" + "BasicHtype" + "\t"
                + String.valueOf(basciTime / queryMPathList.size()) + "ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(basicInfo, 3);
//
        String advanceInfo = "Sca:" + part + "\t" + dataSetName + "\t" + "AdvancedType3" + "\t"
                + String.valueOf(advanceTime / queryMPathList.size()) + "ms" + "\t" + Integer.toString(queryK);
        LogFinal.log(advanceInfo, 3);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("music");
//        dataSetList.add("smallimdb");
//        dataSetList.add("DBLPWithWeight");
//        dataSetList.add("tmdb");
        dataSetList.add("DBPedia");
        int[] kArry = new int[]{5};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            scalabilityTestH vdTest = new scalabilityTestH(graphDataSetPath, metaPathsPath, dataSetName);
            for (int i = 1; i <= 4; i++) {
                vdTest.testAll(5, i, 5);
            }
            LogFinal.log("\n", 3);
            LogPart.log("\n");
        }
    }
}
