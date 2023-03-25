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

public class effencicyCommH {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private BasicHType basicHType;
//    private Advanced3Type advanced3Type;
    private AdvancedHType advancedHType;


    public effencicyCommH(String graphDataSetPath, String metaPathsPath, String dataSetName) {
        dataReader = new DataReader(graphDataSetPath + "/graph.txt", graphDataSetPath + "/vertex.txt", graphDataSetPath + "/edge.txt", graphDataSetPath + "/weight.txt");
        graph = dataReader.readGraph();
        vertexType = dataReader.readVertexType();
        edgeType = dataReader.readEdgeType();
        weight = dataReader.readWeight();
        this.dataSetName = dataSetName;
        this.queryMPathList = new ArrayList<>();
        getQueryMetaPathSet(metaPathsPath);
    }

    private void initComm3(int queryK, MetaPath metaPath) {
        basicHType = new BasicHType(graph, vertexType, edgeType, weight, queryK, metaPath);
//        advanced3Type = new Advanced3Type(graph, vertexType, edgeType, weight, queryK, metaPath);
        advancedHType = new AdvancedHType(graph, vertexType, edgeType, weight, queryK, metaPath);
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

    public long test(Comm3Type comm3Type, String algorithmInfo, String metaPathName) throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        comm3Type.computeComm(dataSetName);
        long end = System.currentTimeMillis();
        String partInfo  = dataSetName + "\t" + algorithmInfo + "\t" + metaPathName + "\t" + (end - start) ;
        LogPart.log(partInfo);
        return (end - start);
    }

    public void testAll(int queryK) throws ExecutionException, InterruptedException {
        long basciTime = 0;
        long advanceTime = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("1-1-0-2-2-3-0-0-1"));
        for (MetaPath metaPath : queryMPathList) {
            initComm3(queryK, metaPath);
            basciTime += test(basicHType, "BasicTypeH", metaPath.toString());
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            advanceTime += test(advancedHType, "AdvancedTypeH", metaPath.toString());
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
            String basicInfo = dataSetName + "\t" + "BasicTypeH" + "\t"
                    + String.valueOf(basciTime / queryMPathList.size()) ;
            LogFinal.log(basicInfo, 3);

        String advanceInfo = dataSetName + "\t" + "AdvancedTypeH" + "\t"
                + String.valueOf(advanceTime / queryMPathList.size()) + "\t" + "queryK: " + queryK ;
        LogFinal.log(advanceInfo, 3);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("tmdb");
//        dataSetList.add("dblp");
//        dataSetList.add("smallimdb");
        dataSetList.add("DBPedia");
//        dataSetList.add("DBLPWithWeight");
        int[] kArry = new int[]{5, 7, 9 , 11, 13, 15};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root  + "/" + dataSetName;
            String metaPathsPath = Config.root  + "/" +     dataSetName;
            effencicyCommH vdTest = new effencicyCommH(graphDataSetPath, metaPathsPath, dataSetName);
            for (int k : kArry) {
                vdTest.testAll(k);
            }
            LogFinal.log("\n", 3);
            LogPart.log("\n");
        }
    }
}
