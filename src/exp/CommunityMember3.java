package exp;

import advanced.Advanced2Type;
import advanced.Advanced3Type;
import build.BatchSearch;
import build.FastBCore;
import javafx.util.Pair;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CommunityMember3 {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private Advanced2Type advanced2Type;
    private Advanced3Type advanced3Type;

    public CommunityMember3(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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
        advanced2Type = new Advanced2Type(graph, vertexType, edgeType, weight, queryK, metaPath);
        advanced3Type = new Advanced3Type(graph, vertexType, edgeType, weight, queryK, metaPath);
    }


    public Map<Integer, Set<Integer>> buildSmallHomGraph(Set<Integer> keepSet, MetaPath metaPath) {
//		System.out.println("vertexNum: " + keepSet.size());
        int sum = 0;
        BatchSearch batchSearch = new BatchSearch(graph, vertexType, edgeType, metaPath, null);
        Map<Integer, Set<Integer>> vertexNbMap = new HashMap<Integer, Set<Integer>>();
        for (int curId : keepSet) {
            Set<Integer> pnbSet = batchSearch.collect(curId, keepSet);
            vertexNbMap.put(curId, pnbSet);
            sum += pnbSet.size();
            if (sum > 10000000 && sum % 1000 == 0) System.out.println("edgeNum:" + sum);
        }

        return vertexNbMap;
    }

    private static int[] computeDiameter(Map<Integer, Set<Integer>> graphMap, Set<Integer> aSet, Set<Integer> bSet) {
        int diameter[] = new int[3];

        Diameter tmp = new Diameter(graphMap, aSet);
        diameter[0] = tmp.computeDiameter();

        tmp = new Diameter(graphMap, bSet);
        diameter[1] = tmp.computeDiameter();

        return diameter;
    }

    private void getQueryMetaPathSet(String metaPathsPath) {
        try {

            BufferedReader stdin = new BufferedReader(new FileReader(metaPathsPath + "/meta3Paths.txt"));
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

    public List<Long> testProcess(List<Pair<Integer, Integer>> querySet, HashMap<Integer, Set<Integer>> eSetMap, MetaPath metaPath, Integer queryK) throws ExecutionException, InterruptedException {
        // Three information needs to record
        // 1. The number of communities
        // 2. The number of nodes in communities
        // 3. The number of nodes in k-core
        List<Long> staticsResult = new ArrayList<>();
        FastBCore bCore = new FastBCore(graph, vertexType, edgeType);


        long communitySize = 0; // 1. use for recording the number of communities
        long communityNodes = 0; // 2. use for recording the number of nodes in communities
        long kcoreNodes = 0; // 3. use for recording the number of nodes in k-core
        long maximumNodes = 0; // 4. record the maximal
        long minimumNodes = 0; // 5. record the minimal
        communitySize += eSetMap.size();
        // for all the querylist
        for (int i = 0; i < querySet.size(); i++) {

            int id = querySet.get(i).getKey();
            int number = querySet.get(i).getValue();
            Set<Integer> kcoreSet = bCore.query(id, metaPath, queryK);
            Set<Integer> HICSet = eSetMap.get(number);
            communityNodes += HICSet.size();
            kcoreNodes += kcoreSet.size();
            if (i == 0) {
                minimumNodes = HICSet.size();
            }
            maximumNodes = Math.max(HICSet.size(), maximumNodes);
            minimumNodes = Math.min(HICSet.size(), minimumNodes);

        }
        System.out.println("Commiunity size is :" + communitySize);
        staticsResult.add(communitySize);
        staticsResult.add(communityNodes / querySet.size());
        staticsResult.add(kcoreNodes / querySet.size());
        staticsResult.add(maximumNodes);
        staticsResult.add(minimumNodes);
        LogPart.log(dataSetName + "QueryK: " + queryK + "\t" + " The number of communities:" + "\t" + communitySize + "\t"
                + metaPath.toString() + "\n");
        LogPart.log(dataSetName + "QueryK: " + queryK + "\t" + " The number of nodes in communities:" + "\t" + staticsResult.get(1) + "\t"
                + metaPath.toString() + "\n");
        LogPart.log(dataSetName + "QueryK: " + queryK + "\t" + " The number of nodes in k-cores:" + "\t" + staticsResult.get(2) + "\t"
                + metaPath.toString() + "\n");
        LogPart.log(dataSetName + "QueryK: " + queryK + "\t" + " The maximum number of nodes in communities:" + "\t" + staticsResult.get(3) + "\t"
                + metaPath.toString() + "\n");
        LogPart.log(dataSetName + "QueryK: " + queryK + "\t" + "The minumul number of nodes in communities:" + "\t" + staticsResult.get(4) + "\t"
                + metaPath.toString() + "\n");
        return staticsResult;
    }


    public void testAll(int queryK) throws ExecutionException, InterruptedException {

        double communitySize = 0; // 1. use for recording the number of communities
        double communityNodes = 0; // 2. use for recording the number of nodes in communities
        double kcoreNodes = 0; // 3. use for recording the number of nodes in k-core
        double maximumNodes = 0; // 4. record the maximal
        double minimumNodes = 0; // 5. record the minimal
        int cnt = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("1-3-0-0-1"));
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
//            Map<double[], Set<Integer>> setMap = improvedAdvanced2Type.computeComm(dataSetName); // for h = 2
            Map<double[], Set<Integer>> setMap = advanced3Type.computeComm(dataSetName); // for h = 3
            if (setMap.isEmpty()) {
                continue;
            }
            cnt += 1;
            List<Pair<Integer, Integer>> querySet = new ArrayList<>();
            HashMap<Integer, Set<Integer>> eSetMap = new HashMap<>();

            int number = 1;
            for (Map.Entry<double[], Set<Integer>> entries : setMap.entrySet()) {
                //1. find keynode u
                int id = entries.getValue().iterator().next();
                Set<Integer> idSet = new HashSet<>();
                for (Integer key : entries.getValue()) {
                    if (weight[id] < weight[key]) {
                        id = key;
                    }
//                    if (weight[key] == entries.getKey()[0]) {
//                        idSet = entries.getValue();
//                    }
                }
                querySet.add(new Pair<>(id, number));
                eSetMap.put(number, entries.getValue());
                number += 1;

            }
            List<Long> results = testProcess(querySet, eSetMap, metaPath, queryK);
            // for results, 0 :  the number of communities, 1 : The number of nodes in communities, 2 :  The number of nodes in k-core, 3 :  The maximum 4 : the minimal
            communitySize += results.get(0);
            communityNodes += results.get(1);
            kcoreNodes += results.get(2);
            maximumNodes += results.get(3);
            minimumNodes += results.get(4);

        }
        LogFinal.log(dataSetName + "\t" + "queryK: " + queryK + "\n" +
                        " The number of communities   " + LogFinal.format(communitySize / cnt) + "\n"
                        + "The average number of nodes in communities " + LogFinal.format(communityNodes / cnt) + "\n" +
                        " The average number of nodes in k-core" + LogFinal.format(kcoreNodes / cnt) + "\n"
                        + "The maximum number of nodes in communities " + LogFinal.format(maximumNodes / cnt) + "\n"
                        + "minimum number of nodes in communities " + LogFinal.format(minimumNodes / cnt) + "\n"
                , 3);
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("music");
        dataSetList.add("tmdb");
        dataSetList.add("smallimdb");
        dataSetList.add("DBPedia");

        dataSetList.add("DBLPWithWeight");

        int[] kArry = new int[]{5,7,9,11,13, 15};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            CommunityMember3 vdTest = new CommunityMember3(graphDataSetPath, metaPathsPath, dataSetName);
            for (int k : kArry) {
                vdTest.testAll(k);
            }
            LogFinal.log("\n", 3);
            LogPart.log("\n");
        }
    }
}
