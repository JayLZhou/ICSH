package exp;

import advanced.Advanced2Type;
import basic.Comm2Type;
import basic.InfComm2Type;
import build.BatchSearch;
import build.FastBCore;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CaseStudy {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private InfComm2Type infComm2Type;
    private Advanced2Type advanced2Type;
    private SmallGraph sGraph;


    public CaseStudy(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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

    public long test(Comm2Type comm2Type, String algorithmInfo, String metaPathName, String queryK, String part) throws ExecutionException, InterruptedException, IOException {
        long start = System.currentTimeMillis();
        Map<double[], Set<Integer>> result = comm2Type.computeComm(dataSetName);

        result.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    ");
            for (Integer x : value) {
                System.out.printf(sGraph.oldVidMap.get(x) - 409340 + "| " + weight[sGraph.oldVidMap.get(x)] + " ");
            }
            System.out.println("-----");
        });

        // compute metric
//         two query id 13795 , 16275
        int[] querySet = new int[]{13795, 75746, 6206, 2449, 21006, 28517, 5681, 16275};
//        int id = querySet.get(i);
        Set<Integer> X = new HashSet<>();
        int cnt = 0;
        double authorWeight = 0.0;
        double paperWeight = 0.0;
        FastBCore bCore = new FastBCore(sGraph.smallGraph, sGraph.smallGraphVertexType, sGraph.smallGraphEdgeType);
        for (int id : querySet) {
            Set<Integer> bSet = bCore.query(sGraph.newVidMap.get(id + 409340), new MetaPath("1-3-0-0-1"), 5);
            paperWeight +=  buildSmallHomGraph(bSet, new MetaPath("1-3-0-0-1"));

            cnt += bSet.size();
            for (int x : bSet) {
//                minf2 = Double.min(minf2, weight[sGraph.oldVidMap.get(x)]);
                authorWeight +=  weight[sGraph.oldVidMap.get(x)];
                if (weight[sGraph.oldVidMap.get(x)] == 0.0) {
                    X.add(sGraph.oldVidMap.get(x) - 409340);
                    System.out.println(sGraph.oldVidMap.get(x) - 409340);
                }

//                X
            }

        }
        System.out.println("avg author:" + authorWeight / cnt);
        System.out.println("avg paper:" + paperWeight / querySet.length);
//        System.out.println("all size is :" + X.size());
//
        long end = System.currentTimeMillis();
//        String partInfo = "scalabilityTest: " + part + "\t" + dataSetName + "\t" + algorithmInfo + "\t" + metaPathName + "\t" + queryK + "\t" + (end - start) + "ms";
//        LogPart.log(partInfo);
        return (end - start);
    }

    public double buildSmallHomGraph(Set<Integer> keepSet, MetaPath metaPath) {
//		System.out.println("vertexNum: " + keepSet.size());
        int sum = 0;
        double allPaper = 0;
        BatchSearch batchSearch = new BatchSearch(graph, vertexType, edgeType, metaPath, null);
        Map<Integer, Set<Integer>> vertexNbMap = new HashMap<Integer, Set<Integer>>();
        for (int curId : keepSet) {
            Set<Integer> pnbSet = batchSearch.collect(curId, keepSet);
            vertexNbMap.put(curId, pnbSet);
            sum += pnbSet.size();
            if (sum > 10000000 && sum % 1000 == 0) System.out.println("edgeNum:" + sum);
        }
        double minf2 = Double.MAX_VALUE;
        for (int id : batchSearch.type2) {
            allPaper += weight[sGraph.oldVidMap.get(id)];
        }
//        System.out.println("paper:" + minf2);
        return allPaper / batchSearch.type2.size();
    }

    public void testAll(int queryK, int part, int total) throws ExecutionException, InterruptedException, IOException {
        HashSet<Integer> querySet = new HashSet<>();
        // solve the case for A-P-A
        int[] add = new int[]{
                42200, 19321, 42199, 4992, 103066, 400011, 30526, 103065, 107311, 21135, 21136, 169636, 115876, 169635, 49311, 97950, 72371, 33970, 110790, 397495, 8371, 59580, 8375, 26320, 5856, 167150, 167156, 172290, 261875, 154375, 167190, 105745, 210716, 284965, 98071, 210715, 284966, 39201, 105765, 108326, 39205, 36651, 39211, 39206, 284990, 267070, 39210, 167225, 284991, 267071, 836, 8516, 21315, 49490, 366931, 21336, 254325, 44405, 34171, 46975, 346501, 121216, 346505, 121215, 346506, 346510, 346500, 18830, 52110, 167330, 346511, 346515, 67495, 34221, 167350, 103356, 62395, 390081, 167356, 139210, 41916, 397756, 390080, 34255, 139211, 21455, 18906, 159716, 36835, 34270, 108530, 47085, 195566, 400356, 36836, 13795, 36840, 254461, 41975, 75261, 54795, 44555, 287761, 198156, 54796, 287765, 198155, 287766, 29186, 287770, 280076, 34320, 287760, 29205, 123931, 287771, 54826, 287775, 18971, 287776, 3626, 1066, 108610, 167485, 108620, 1086, 131656, 167495, 131655, 44611, 62540, 111185, 195665, 205905, 49756, 195681, 44660, 3706, 3705, 149641, 170135, 52381, 42155, 8861, 37026, 118951, 19120, 170166, 170165, 159925, 39610, 167635, 21706, 200930, 159966, 108776, 131811, 42226, 152305, 44781, 167680, 49911, 167686, 167685, 147201, 267520, 167681, 16641, 3845, 162571, 6411, 72981, 198436, 198435, 24345, 21786, 44840, 400701, 167750, 400700, 167755, 198490, 26960, 198486, 26961, 96081, 198485, 52575, 108896, 108895, 108910, 195950, 116590, 303481, 108916, 37230, 119151, 21866, 111481, 47490, 157571, 106386, 170390, 50066, 106385, 108945, 167840, 47516, 108951, 211351, 108970, 65446, 167846, 167845, 167841, 167860, 37295, 167856, 167865, 167861, 50111, 29640, 9160, 29641, 1480, 101320, 149961, 29636, 21965, 29645, 106465, 170465, 45045, 129536, 111626, 60426, 288281, 27161, 50206, 45080, 127005, 45081, 47655, 1570, 47656, 1565, 267830, 121891, 150065, 170560, 206400, 167995, 16941, 267831, 170561, 206401, 32331, 40015, 40016, 168025, 34900, 232036, 1636, 47726, 93801, 104056, 104055, 42610, 24700, 42626, 401041, 73356, 162966, 109205, 401040, 73370, 22165, 22166, 152731, 9376, 45246, 50360, 19645, 1731, 196301, 45261, 111821, 22230, 22240, 106725, 168166, 35040, 9450, 14565, 52985, 29950, 35081, 163086, 163085, 35080, 163081, 106776, 106786, 106785, 42780, 106781, 268056, 206625, 106780, 17181, 101686, 14635, 101680, 165696, 24885, 222025, 111945, 111941, 106845, 1875, 35185, 40296, 35180, 42885, 101765, 196485, 42881, 94096, 339855, 106901, 339856, 101780, 24996, 332221, 332226, 1961, 35261, 206785, 168381, 168400, 106956, 106955, 106951, 45525, 35281, 42966, 42960, 196561, 234980, 153056, 153055, 106970, 71145, 168430, 234981, 168440, 25066, 168446, 255475, 168445, 168441, 119815, 9725, 50706, 114720, 43030, 168486, 168485, 168481, 229950, 45625, 53316, 107075, 229956, 229955, 229951, 25160, 32836, 2125, 99420, 22625, 37990, 125050, 43130, 66171, 398975, 20096, 166035, 45725, 45726, 166056, 99490, 17565, 25261, 17575, 166080, 196800, 212156, 71356, 107190, 166090, 166086, 153285, 166085, 166081, 32975, 30405, 166091, 2245, 30406, 15051, 401641, 401645, 178915, 401646, 401640, 166135, 158465, 27940, 53546, 135466, 43301, 171330, 166210, 107326, 196925, 340276, 10035, 127801, 63816, 48451, 266051, 166240, 109920, 35671, 48470, 102231, 109930, 235380, 94585, 15250, 40855, 150935, 15245, 40861, 10140, 40856, 171425, 268700, 40860, 102316, 166320, 102315, 30641, 171450, 171446, 17831, 171445, 166321, 161220, 168896, 166336, 168895, 171451, 12731, 30675, 166355, 17880, 35815, 48616, 253425, 17890, 127976, 166375, 253426, 166390, 35825, 30705, 23021, 35816, 171530, 366075, 35836, 168975, 48660, 7691, 30740, 107541, 7695, 169000, 46115, 10280, 69170, 169040, 53835, 69190, 261195, 169041, 10336, 153705, 10335, 148581, 10350, 2661, 82030, 112770, 23161, 10351, 28275, 107641, 15475, 23175, 102556, 350881, 102555, 350885, 169115, 350886, 46231, 350890, 110250, 30885, 350876, 350880, 33451, 350901, 350905, 169136, 23205, 350906, 23206, 350910, 5296, 28340, 350891, 46265, 107706, 350895, 350896, 171705, 350900, 166600, 350921, 350925, 302286, 350926, 33470, 350930, 166591, 166610, 350911, 350915, 166605, 350916, 18111, 107721, 166601, 350920, 350941, 350945, 33486, 350946, 166611, 350950, 256230, 350931, 371411, 350935, 350936, 350940, 110301, 171756, 171755, 5345, 350951, 350955, 20715, 107761, 197376, 197375, 235790, 48900, 2815, 194836, 166671, 5385, 5400, 10520, 28435, 10521, 28455, 48956, 112951, 105285, 97605, 171845, 23366, 166750, 171870, 166755, 48990, 48991, 166770, 8040, 46440, 120700, 41331, 171900, 125816, 64371, 197510, 171901, 151435, 197511, 169370, 169376, 169375, 169371, 38810, 20900, 151465, 166825, 18350, 2985, 200130, 59321, 43961, 20930, 46536, 200135, 235971, 200131, 128470, 10700, 266721, 171996, 169436, 166871, 20960, 169446, 10711, 169445, 266720, 23531, 41460, 72196, 200205, 197645, 159241, 397341, 8206, 21006, 397340, 546, 123436, 154155, 123435, 23595, 200276, 581, 38996, 123501, 400001, 44160, 400000, 8315
                ,13795, 75746, 6206, 2449, 21006, 28517, 5681, 16275,269532, 28855,203811,170204

        };
        int offset = 409340;
        // use for datamining
        querySet.add(30526 + offset); // add Xuemin Lin
        querySet.add(107311 + offset); // add Jeffrey Xu Yu
        querySet.add(42199 + offset); // add Luqin
        querySet.add(106224 + offset); // add Luqin
        querySet.add(42200 + offset); // add wenjie Zhang
        querySet.add(4992 + offset); // add Hanjia Wei
        querySet.add(19321 + offset); // add Sunyi Zhou
        for (int x : add) {
            querySet.add(x + offset);
        }
        querySet.add(4137 + offset); // add michke

//        querySet.add(20259 + offset); // add michke


        sGraph.getSmallGraph(part, total, querySet);
//        sGraph.


        long basciTime = 0;
        long advanceTime = 0;
        long imporveAdvanceTime = 0;
        int validCnt = 0;
        queryMPathList.clear();
        queryMPathList.add(new MetaPath("1-3-0-0-1"));
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
            System.out.println("meta Path is : " + metaPath.toString());
            long improTestTime = test(advanced2Type, "ImprovedAdvancedType2", metaPath.toString(), Integer.toString(queryK), Integer.toString(part));
            if (improTestTime != 0) {
                imporveAdvanceTime += improTestTime;
                validCnt += 1;
            }
            try {
                System.gc();
                Thread.sleep(10000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
//        String improveAdvanceInfo = "Sca: " + part + "\t" + dataSetName + "\t" + "ImprovedAdvancedType2" + "\t"
//                + String.valueOf(imporveAdvanceTime / validCnt) + "ms" + "\t" + Integer.toString(queryK);
//        LogFinal.log(improveAdvanceInfo, 2);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("DBLPWithWeight");
//        dataSetList.add("tmdb");
//        dataSetList.add("music");
        dataSetList.add("DBLPWEIGHTWHOLE");
//        dataSetList.add("DBPedia");/**/
        int[] kArry = new int[]{5, 7, 9, 11, 13, 15};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            CaseStudy vdTest = new CaseStudy(graphDataSetPath, metaPathsPath, dataSetName);
            for (int i = 2; i <= 2; i++) {
                vdTest.testAll(5, 21, 30);
            }
            LogFinal.log("\n", 2);
            LogPart.log("\n");
        }
    }
}

