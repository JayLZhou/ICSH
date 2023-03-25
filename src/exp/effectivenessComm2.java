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

public class effectivenessComm2 {
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private advanced.Advanced2Type Advanced2Type;
    private Advanced3Type advanced3Type;

    public effectivenessComm2(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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
        Advanced2Type = new Advanced2Type(graph, vertexType, edgeType, weight, queryK, metaPath);
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

    public List<double[]> testProcess(List<Pair<Integer, Integer>> querySet, HashMap<Integer, Set<Integer>> eSetMap, MetaPath metaPath) throws ExecutionException, InterruptedException {
        List<double[]> staticsResult = new ArrayList<>();
        FastBCore bCore = new FastBCore(graph, vertexType, edgeType);

        List<double[]> sList = new ArrayList<double[]>(); // use for path-sim
        for (int j = 0; j < querySet.size(); j++) {
            sList.add(new double[2]);
        }
        List<int[]> dList = new ArrayList<int[]>(); // use for diameter
        List<double[]> dseList = new ArrayList<double[]>(); // use for dense
        List<double[]> degList = new ArrayList<double[]>(); // use for degree
        // for all the querylist
        for (int i = 0; i < querySet.size(); i++) {
            int id = querySet.get(i).getKey();
            int number = querySet.get(i).getValue();
            Set<Integer> bSet = bCore.query(id, metaPath, 5);
            Map<Integer, Set<Integer>> graphMap = buildSmallHomGraph(bSet, metaPath);
            // compute diameter
            int diameter[] = computeDiameter(graphMap, bSet, eSetMap.get(number));
            // compute degree
            double degree[] = computeDegree(graphMap, bSet, eSetMap.get(number));
            dList.add(diameter);
            degList.add(degree);
            Map<Integer, Map<Integer, Integer>> psimMap = batchBuildForPathSim(bSet, metaPath);
            // compute the pathsim
            double sim[] = new double[2];
            // compute for bcore
            double bPathSim = avgPathSim(bSet, psimMap);
            sim[0] = bPathSim;
            //compute for inf-core
            double ePathSim = avgPathSim(eSetMap.get(number), psimMap);
            sim[1] = ePathSim;

            System.out.println(i + " th query pathsim obtained.");
            sList.set(i, sim);

//             compute density
            double density[] = computeDensity(bSet, eSetMap.get(number), metaPath);
            dseList.add(density);
        }
        // 1. compute diameter
        double bDiameter = 0, eDiameter = 0;
        for (int diameter[] : dList) {
            bDiameter += diameter[0];
            eDiameter += diameter[1];
        }
        bDiameter /= dList.size();
        eDiameter /= dList.size();
        LogPart.log(dataSetName + " community diameter " + LogFinal.format(bDiameter) + "\t" + LogFinal.format(eDiameter) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{bDiameter, eDiameter}); // add diameter to result
        // 2. compute path-sim
        double bPathSim = 0, ePathSim = 0;
        for (double sim[] : sList) {
            bPathSim += sim[0];
            ePathSim += sim[1];

        }
        bPathSim /= sList.size();
        ePathSim /= sList.size();
        LogPart.log(dataSetName + " community PathSim " + LogFinal.format(bPathSim) + "\t" + LogFinal.format(ePathSim) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{bPathSim, ePathSim}); //  add path-sim to result
        // 3. compute density
        double bDensity = 0, eDensity = 0;
        for (double density[] : dseList) {
            bDensity += density[0];
            eDensity += density[1];

        }
        bDensity /= dseList.size();
        eDensity /= dseList.size();


        LogPart.log(dataSetName + " community density " + LogFinal.format(bDensity) + "\t" + LogFinal.format(eDensity) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{bDensity, eDensity}); //  add density to result

        // 4. compute degree
        double bDegree = 0, eDegree = 0;
        for (double deg[] : degList) {
            bDegree += deg[0];
            eDegree += deg[1];
        }
        bDegree /= dList.size();
        eDegree /= dList.size();
        LogPart.log(dataSetName + " community degree " + LogFinal.format(bDegree) + "\t" + LogFinal.format(eDegree) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{bDegree, eDegree}); //  add degree to result

        return staticsResult;
    }


    private double[] computeDegree(Map<Integer, Set<Integer>> graphMap, Set<Integer> bSet, Set<Integer> eSet) {
        double bSum = 0, eSum = 0;
        for (Integer bkey : bSet) {
            bSum += graphMap.get(bkey).size();
        }
        for (Integer eKey : eSet) {
            eSum += graphMap.get(eKey).size();
        }

        double degree[] = {bSum / bSet.size(), eSum / eSet.size()};
        return degree;
    }

    //compute the average numbers of linked vertices
    private double[] computeDensity(Set<Integer> bSet, Set<Integer> eSet, MetaPath metaPath) {
        double bSum = 0, eSum = 0;

        for (int curId : bSet) {
            bSum += countPaths(bSet, curId, metaPath);
        }
        for (int curId : eSet) {
            eSum += countPaths(bSet, curId, metaPath);
        }
        //compute the total number of edges
        bSum /= 2;
        eSum /= 2;

        double density[] = {bSum / bSet.size(), eSum / eSet.size()};
        return density;
    }

    private double countPaths(Set<Integer> keepSet, int vertexId, MetaPath metaPath) {
        int pathLen = metaPath.pathLen;
        Map<Integer, Integer> batchMap = new HashMap<Integer, Integer>();
        batchMap.put(vertexId, 1);
        for (int layer = 0; layer < pathLen; layer++) {
            int targetVType = metaPath.vertex[layer + 1], targetEType = metaPath.edge[layer];
            Map<Integer, Integer> nextBatchMap = new HashMap<Integer, Integer>();
            for (Map.Entry<Integer, Integer> entry : batchMap.entrySet()) {
                int anchorId = entry.getKey();
                int anchorCount = entry.getValue();

                int nbArr[] = graph[anchorId];
                for (int i = 0; i < nbArr.length; i += 2) {
                    int nbVertexID = nbArr[i], nbEdgeID = nbArr[i + 1];
                    if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                        if (layer < metaPath.pathLen - 1) {
                            if (!nextBatchMap.containsKey(nbVertexID)) {
                                nextBatchMap.put(nbVertexID, anchorCount);
                            } else {
                                int exiting = nextBatchMap.get(nbVertexID);
                                nextBatchMap.put(nbVertexID, exiting + anchorCount);
                            }
                        } else {
                            if (nbVertexID != vertexId && keepSet.contains(nbVertexID)) {
                                if (!nextBatchMap.containsKey(nbVertexID)) {
                                    nextBatchMap.put(nbVertexID, anchorCount);
                                } else {
                                    int existing = nextBatchMap.get(nbVertexID);
                                    nextBatchMap.put(nbVertexID, existing + anchorCount);
                                }
                            }
                        }
                    }
                }
            }

            batchMap = nextBatchMap;
        }

        if (batchMap.size() > 100000000) System.out.println("|batchMap|=" + batchMap.size());
        double sum = 0;
        for (Map.Entry<Integer, Integer> entry : batchMap.entrySet()) {
            sum += entry.getValue();
        }

        return sum;
    }

    public void testAll(int queryK) throws ExecutionException, InterruptedException {
        long advanceTime = 0;
        long imporveAdvanceTime = 0;
        double bDiameter = 0, eDiameter = 0;
        double bDegree = 0, eDegree = 0;
        double bDensity = 0, eDensity = 0;
        double bPath = 0, ePath = 0;
//        queryMPathList.clear();
//        queryMPathList.add(new MetaPath("1-3-0-0-1"));
        int cnt = 0;
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
            Map<double[], Set<Integer>> setMap = Advanced2Type.computeComm(dataSetName); // for h = 2
//            Map<double[], Set<Integer>> setMap = advanced3Type.computeComm(dataSetName); // for h = 3
            if (setMap.isEmpty()) {
                continue;
            } else {
                cnt += 1;
            }

            List<Pair<Integer, Integer>> querySet = new ArrayList<>();
            Set<Integer> keys = new HashSet<>();
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
                keys.add(id);
                querySet.add(new Pair<>(id, number));

                eSetMap.put(number, entries.getValue());
                number += 1;

            }

            List<double[]> results = testProcess(querySet, eSetMap, metaPath);
            // for results, 0 : diameter, 1 : path, 2 : density, 3 : degree
            bDiameter += results.get(0)[0];
            eDiameter += results.get(0)[1];
            bPath += results.get(1)[0];
            ePath += results.get(1)[1];
            bDensity += results.get(2)[0];
            eDensity += results.get(2)[1];
            bDegree += results.get(3)[0];
            eDegree += results.get(3)[1];
        }
        LogFinal.log(dataSetName + " community diameter " + LogFinal.format(bDiameter / cnt) + "\t" + LogFinal.format(eDiameter / cnt) + "\n"
                        + " community path-sim " + LogFinal.format(bPath / cnt) + "\t" + LogFinal.format(ePath / cnt) + "\n" +
                        " community density " + LogFinal.format(bDensity / cnt) + "\t" + LogFinal.format(eDensity / cnt) + "\n" +
                        " community degree " + LogFinal.format(bDegree / cnt) + "\t" + LogFinal.format(eDegree / cnt)
                , 2);
    }

    public Map<Integer, Map<Integer, Integer>> batchBuildForPathSim(Set<Integer> keepSet, MetaPath queryMPath) {
        Map<Integer, Map<Integer, Integer>> vertexNbMap = new HashMap<Integer, Map<Integer, Integer>>();
        for (int startId : keepSet) {
            Map<Integer, Integer> anchorMap = new HashMap<Integer, Integer>();
            anchorMap.put(startId, 1);
            for (int layer = 0; layer < queryMPath.pathLen; layer++) {
                int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];
                Map<Integer, Integer> nextAnchorMap = new HashMap<Integer, Integer>();
                for (int anchorId : anchorMap.keySet()) {
                    int anchorPNum = anchorMap.get(anchorId);
                    int nb[] = graph[anchorId];
                    for (int i = 0; i < nb.length; i += 2) {
                        int nbVertexId = nb[i], nbEdgeId = nb[i + 1];
                        if (targetVType == vertexType[nbVertexId] && targetEType == edgeType[nbEdgeId]) {
                            if (layer < queryMPath.pathLen - 1) {
                                if (!nextAnchorMap.containsKey(nbVertexId)) nextAnchorMap.put(nbVertexId, 0);
                                int curPNum = nextAnchorMap.get(nbVertexId);
                                nextAnchorMap.put(nbVertexId, anchorPNum + curPNum);
                            } else {
                                if (keepSet.contains(nbVertexId)) {
                                    if (!nextAnchorMap.containsKey(nbVertexId)) nextAnchorMap.put(nbVertexId, 0);
                                    int curPNum = nextAnchorMap.get(nbVertexId);
                                    nextAnchorMap.put(nbVertexId, anchorPNum + curPNum);
                                }
                            }
                        }
                    }
                }
                anchorMap = nextAnchorMap;
            }
            vertexNbMap.put(startId, anchorMap);
        }
        return vertexNbMap;
    }

    public double avgPathSim(Set<Integer> coreSet, Map<Integer, Map<Integer, Integer>> psimMap) {
        int eNum = 0;
        double cSim = 0;
        for (int vid : coreSet) {
            Map<Integer, Integer> nbMap = psimMap.get(vid);
            for (int nbVid : nbMap.keySet()) {
                if (!coreSet.contains(nbVid)) continue;
                if (nbVid <= vid) continue;
                eNum++;
                cSim += ((double) psimMap.get(vid).get(nbVid) + (double) psimMap.get(vid).get(nbVid)) / ((double) psimMap.get(vid).get(vid) + (double) psimMap.get(nbVid).get(nbVid));
            }
        }
        eNum = coreSet.size() * (coreSet.size() - 1) / 2;
        cSim = cSim / eNum;
        return cSim;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Specify the dataset
        List<String> dataSetList = new ArrayList<String>();
        dataSetList.add("tmdb");
        dataSetList.add("smallimdb");
        dataSetList.add("DBPedia");

        dataSetList.add("DBLPWithWeight");

        int[] kArry = new int[]{5};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + dataSetName;
            String metaPathsPath = Config.root + dataSetName;
            effectivenessComm2 vdTest = new effectivenessComm2(graphDataSetPath, metaPathsPath, dataSetName);
            for (int k : kArry) {
                vdTest.testAll(k);
            }
            LogFinal.log("\n", 2);
            LogPart.log("\n");
        }
    }
}
