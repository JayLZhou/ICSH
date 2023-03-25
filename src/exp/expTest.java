package exp;

import advanced.Advanced2Type;
import advanced.Basic2Plus;
import basic.Comm2Type;
import basic.InfComm2Type;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// This class is provided to reproduce all our experiments
public class expTest {
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


    public expTest(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        List<String> dataSetList = new ArrayList<String>();
        // Register the four datasets
        dataSetList.add("tmdb");
        dataSetList.add("DBLPWithWeight");
        dataSetList.add("smallimdb");
        dataSetList.add("DBPedia");
        int[] kArry = new int[]{11, 13, 15};
        // 1. runEfficiency: reproduce the whole efficiency experiments
        // 2. runEffectiveness: reproduce the whole effectiveness experiments (i.e., diameter, path-sim, etc.)
        // 3. runMember: reproduce the whole size and number experiments
        // 4. runAvgInfluence: reproduce the average influence experiments
        // 5. runCaseStudy: reproduce the case-study experiment
        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            expTest vdTest = new expTest(graphDataSetPath, metaPathsPath, dataSetName);
            vdTest.runEfficiency(graphDataSetPath, metaPathsPath, dataSetName, kArry);

            vdTest.runEffectivenss(graphDataSetPath, metaPathsPath, dataSetName, 5); // 5 is the default queryK.
            vdTest.runMember(graphDataSetPath, metaPathsPath, dataSetName, kArry);
            if (dataSetName.equals("tmdb") || dataSetName.equals("DBLPWithWeight")) {
                vdTest.runAvgInfluence(graphDataSetPath, metaPathsPath, dataSetName, 5); // Only for TMDB (MDM and GMDMG) and DBLP (APA and TPVPT)
            }
            // CaseStudy only for the DBLP (APA)
            if (dataSetName.equals("DBLPWithWeight")) {
                vdTest.runCaseStudy(graphDataSetPath, metaPathsPath, dataSetName);
            }
        }

    }

    private void runCaseStudy(String graphDataSetPath, String metaPathsPath, String dataSetName) throws IOException, ExecutionException, InterruptedException {
        CaseStudy caseStudy = new CaseStudy(graphDataSetPath, metaPathsPath, dataSetName);
        for (int i = 2; i <= 2; i++) {
            caseStudy.testAll(5, 21, 30);
        }
    }

    private void runAvgInfluence(String graphDataSetPath, String metaPathsPath, String dataSetName, int k) throws ExecutionException, InterruptedException {
        avgInfValue vdTest = new avgInfValue(graphDataSetPath, metaPathsPath, dataSetName);
        vdTest.testAll(k);

    }

    private void runMember(String graphDataSetPath, String metaPathsPath, String dataSetName, int [] kArry) throws ExecutionException, InterruptedException {
        CommunityMember2 mem2Test = new CommunityMember2(graphDataSetPath, metaPathsPath, dataSetName);
        CommunityMember3 mem3Test = new CommunityMember3(graphDataSetPath, metaPathsPath, dataSetName);
        // run the member and size for h=2
        for (int k : kArry) {
            mem2Test.testAll(k);
        }
        // run the member and size for h=3
        for (int k : kArry) {
            mem3Test.testAll(k);
        }
    }

    private void runEffectivenss(String graphDataSetPath, String metaPathsPath, String dataSetName, int k) throws ExecutionException, InterruptedException {
        effectivenessComm2 effness2Test = new effectivenessComm2(graphDataSetPath, metaPathsPath, dataSetName);
        effectivenessComm3 effness3Test = new effectivenessComm3(graphDataSetPath, metaPathsPath, dataSetName);
        // Run all the effectiveness  for h=2
        effness2Test.testAll(k);
        // Run all the effectiveness for h=3
        effness3Test.testAll(k);

    }


    private void runEfficiency(String graphDataSetPath, String metaPathsPath, String dataSetName, int[] kArry) throws ExecutionException, InterruptedException {
        effencicyComm2 eff2Test = new effencicyComm2(graphDataSetPath, metaPathsPath, dataSetName);
        effencicyComm3 eff3Test = new effencicyComm3(graphDataSetPath, metaPathsPath, dataSetName);
        effencicyCommH eff4Test = new effencicyCommH(graphDataSetPath, metaPathsPath, dataSetName);
        // Run all the efficiency  for h=2
        for (int k : kArry) {
            eff2Test.testAll(k);
        }
        // Run all the efficiency for h=3
        for (int k : kArry) {
            eff3Test.testAll(k);
        }
        // Run all the efficiency for h=4
        for (int k : kArry) {
            eff4Test.testAll(k);
        }
    }
}
