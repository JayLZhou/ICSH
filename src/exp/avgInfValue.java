package exp;

import advanced.Advanced2Type;
import advanced.Advanced3Type;
import basic.InfComm2Type;
import build.BatchSearch;
import build.FastBCore;
import javafx.util.Pair;
import util.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class avgInfValue{
    private DataReader dataReader;
    private String dataSetName;
    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;
    private List<MetaPath> queryMPathList;
    private Advanced3Type advanced3Type;
    private InfComm2Type infComm2Type;
    private Advanced2Type advanced2Type;

    public avgInfValue(String graphDataSetPath, String metaPathsPath, String dataSetName) {
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

    public List<double[]> testProcess(List<Pair<Integer, Integer>> querySet, HashMap<Integer, Set<Integer>> eSetMap, MetaPath metaPath, HashMap<Integer, double[]> eSetMapValue) throws ExecutionException, InterruptedException {
        List<double[]> staticsResult = new ArrayList<>();
        FastBCore bCore = new FastBCore(graph, vertexType, edgeType);

        List<double[]> sList = new ArrayList<double[]>(); // use for path-sim
        for (int j = 0; j < querySet.size(); j++) {
            sList.add(new double[2]);
        }
        List<int[]> dList = new ArrayList<int[]>(); // use for diameter
        List<double[]> dseList = new ArrayList<double[]>(); // use for dense
        List<double[]> degList = new ArrayList<double[]>(); // use for degree
        List<double[]> avgType1List = new ArrayList<double[]>(); // use for teyp1-importance
        List<double[]> avgType2List = new ArrayList<double[]>(); // use for  teyp2-importance
        List<double[]> avgType3List = new ArrayList<double[]>(); // use for  teyp3-importance
        // for all the querylist
        for (int i = 0; i < querySet.size(); i++) {
            int id = querySet.get(i).getKey();
            int number = querySet.get(i).getValue();

            Set<Integer> bSet = bCore.query(id, metaPath, 5);
            // compute first-type importance value
            avgType1List.add(computeAvgImportance(bSet, eSetMap.get(number), metaPath, 1, eSetMapValue.get(number)));

//            double[] avgImporance = computeAvgImportancePair(bSet, eSetMap.get(number), metaPath, eSetMapValue.get(number));
            double[] avgImporance = computeAvgImportanceNonMetaPath(bSet, eSetMap.get(number), metaPath, eSetMapValue.get(number));
            for (int j = 0; j < avgImporance.length; j += 2) {
                if (j < 2) {
                    avgType2List.add(new double[]{avgImporance[j], avgImporance[j + 1]});
                } else {
                    avgType3List.add(new double[]{avgImporance[j], avgImporance[j + 1]});
                }
            }
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
        bDegree /= degList.size();
        eDegree /= degList.size();
        LogPart.log(dataSetName + " community degree " + LogFinal.format(bDegree) + "\t" + LogFinal.format(eDegree) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{bDegree, eDegree}); //  add degree to result

        // 5. compute type1
        double btype1 = 0, etype1 = 0;
        for (double deg[] : avgType1List) {
            btype1 += deg[0];
            etype1 += deg[1];
        }
        btype1 /= avgType1List.size();
        etype1 /= avgType1List.size();
        LogPart.log(dataSetName + " community type1-avg " + LogFinal.format(btype1) + "\t" + LogFinal.format(etype1) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{btype1, etype1}); //  add degree to result
        // 6. compute type2
        double btype2 = 0, etype2 = 0;
        for (double deg[] : avgType2List) {
            btype2 += deg[0];
            etype2 += deg[1];
        }
        btype2 /= avgType2List.size();
        etype2 /= avgType2List.size();
        LogPart.log(dataSetName + " community type2-avg " + LogFinal.format(btype2) + "\t" + LogFinal.format(etype2) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{btype2, etype2}); //  add degree to result
        // 7.compute type3
        double btype3 = 0, etype3 = 0;
        for (double deg[] : avgType3List) {
            btype3 += deg[0];
            etype3 += deg[1];
        }
        btype3 /= avgType3List.size();
        etype3 /= avgType3List.size();
        LogPart.log(dataSetName + " community type3-avg " + LogFinal.format(btype3) + "\t" + LogFinal.format(etype3) + "\t"
                + metaPath.toString() + "\n");
        staticsResult.add(new double[]{btype3, etype3}); //  add degree to result
        return staticsResult;
    }

    private double[] computeAvgImportancePair(Set<Integer> bSet, Set<Integer> eSet, MetaPath metaPath, double[] infs) {
        int cnt2 = 0;
        int cnt3 = 0;
        double bsum = 0.0;
        double esum = 0.0;

        double bsum1 = 0.0;
        double esum1 = 0.0;

        Set<Integer> bType2s = buildHomGraphAvg(bSet, metaPath, 2);
        Set<Integer> eType2s = buildHomGraphAvg(eSet, metaPath, 2);
        for (Integer bkey : bType2s) {
            bsum += weight[bkey];
        }

        for (Integer ekey : eType2s) {
            if (weight[ekey] >= infs[1]) {
                esum += weight[ekey];
                cnt2 += 1;
            }

        }
        Set<Integer> bType3s = buildHomGraphAvg(bSet, metaPath, 3);
        Set<Integer> eType3s = buildHomGraphAvg(eSet, metaPath, 3);
        for (Integer bkey : bType3s) {
            bsum1 += weight[bkey];
        }

        for (Integer ekey : eType3s) {
            if (weight[ekey] >= infs[2]) {
                esum1 += weight[ekey];
                cnt3 += 1;
            }
        }
        return new double[]{bsum / bType2s.size(), esum / cnt2, bsum1 / bType3s.size(), esum1 / cnt3};
    }

    private double[] computeAvgImportanceNonMetaPath(Set<Integer> bSet, Set<Integer> eSet, MetaPath metaPath, double[] infs) {
        int cnt2 = 0;
        int cnt3 = 0;
        double bsum = 0.0;
        double esum = 0.0;

        double bsum1 = 0.0;
        double esum1 = 0.0;

        Set<Integer> eType2s = buildHomGraphAvg(eSet, metaPath, 2);

        for (Integer ekey : eType2s) {
            if (weight[ekey] >= infs[1]) {
                esum += weight[ekey];
                cnt2 += 1;
            }
            bsum += weight[ekey];

        }
        Set<Integer> eType3s = buildHomGraphAvg(eSet, metaPath, 3);


        for (Integer ekey : eType3s) {
            if (weight[ekey] >= infs[2]) {
                esum1 += weight[ekey];
                cnt3 += 1;
            }
            bsum1 += weight[ekey];
        }
        return new double[]{bsum / eType2s.size(), esum / cnt2, bsum1 / eType3s.size(), esum1 / cnt3};
    }

    private double[] computeAvgImportance(Set<Integer> bSet, Set<Integer> eSet, MetaPath metaPath, int type, double[] infs) {
        int cnt = 0;
        double bsum = 0.0;
        double esum = 0.0;
        Map<Double, Integer> bDistributionMap = new HashMap<>();
        Map<Double, Integer> eDistributionMap = new HashMap<>();

        if (type == 1) {
            for (Integer bkey : bSet) {
                bsum += weight[bkey];
                if (bDistributionMap.containsKey(weight[bkey])) {
                    Integer bcnt = bDistributionMap.get(weight[bkey]);
                    bDistributionMap.replace(weight[bkey], bcnt + 1);
                } else {
                    bDistributionMap.put(weight[bkey], 1);
                }
            }
            for (Integer ekey : eSet) {
//                assert weight[ekey] >= infs[0];
                esum += weight[ekey];
                if (eDistributionMap.containsKey(weight[ekey])) {
                    Integer bcnt = eDistributionMap.get(weight[ekey]);
                    eDistributionMap.replace(weight[ekey], bcnt + 1);
                } else {
                    eDistributionMap.put(weight[ekey], 1);
                }
            }
//            assert
            return new double[]{bsum / bSet.size(), esum / eSet.size()};
        }

        Set<Integer> bType2s = buildHomGraphAvg(bSet, metaPath, 2);
        Set<Integer> eType2s = buildHomGraphAvg(eSet, metaPath, 2);
        for (Integer bkey : bType2s) {
            bsum += weight[bkey];
        }

        for (Integer ekey : eType2s) {
            if (weight[ekey] >= infs[1])
                esum += weight[ekey];
        }
        return new double[]{bsum / bType2s.size(), esum / eType2s.size()};


    }

    public Set<Integer> buildHomGraphAvg(Set<Integer> keepSet, MetaPath metaPath, int type) {

        BatchSearch batchSearch = new BatchSearch(graph, vertexType, edgeType, metaPath, null);

        for (int curId : keepSet) {
            batchSearch.collect(curId, keepSet);
//            batchSearch.collect3Type(curId, keepSet, );
        }
        if (type == 2) {
            return batchSearch.type2;
        } else {
            return batchSearch.type3;
        }
    }





    public void testAll(int queryK) throws ExecutionException, InterruptedException {
        double bType1 = 0, eType1 = 0;
        double bType2 = 0, eType2 = 0;
        double bType3 = 0, eType3 = 0;
        queryMPathList.clear();
        // You can specify the meta-path to compute the avg. influence value.
        queryMPathList.add(new MetaPath("0-6-4-7-0")); // M-D-M for tmdb
//        queryMPathList.add(new MetaPath("1-3-0-0-1")); // A-P-A for dblp
//        queryMPathList.add(new MetaPath("5-9-0-6-4-7-0-8-5")); //G-M-D-M-G for tmdb
//        queryMPathList.add(new MetaPath("5-9-0-6-4-7-0-8-5")); //G-M-D-M-G for tmdb
//        queryMPathList.add(new MetaPath("3-5-0-1-2-4-0-2-3")); //V-P-T-P-V for dblp
        for (MetaPath metaPath : queryMPathList) {
            initComm2(queryK, metaPath);
            Map<double[], Set<Integer>> setMap = advanced2Type.computeComm(dataSetName); // for h = 2
//            Map<double[], Set<Integer>> setMap = advanced3Type.computeComm(dataSetName); // for h = 3
            if (setMap.isEmpty()) {
                continue;
            }


            List<Pair<Integer, Integer>> querySet = new ArrayList<>();
            HashMap<Integer, Set<Integer>> eSetMap = new HashMap<>();
            HashMap<Integer, double[]> eSetMapValue = new HashMap<>();
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
                eSetMapValue.put(number, entries.getKey());
                number += 1;

            }

            List<double[]> results = testProcess(querySet, eSetMap, metaPath, eSetMapValue);
//

            // compute for avg-importance value
            bType1 += results.get(4)[0];
            eType1 += results.get(4)[1];
            bType2 += results.get(5)[0];
            eType2 += results.get(5)[1];
            bType3 += results.get(6)[0];
            eType3 += results.get(6)[1];
        }

        LogFinal.log(dataSetName + " community type-1 " + LogFinal.format(bType1) + "\t" + LogFinal.format(eType1) + "\n"
                        + " community type-2 " + LogFinal.format(bType2) + "\t" + LogFinal.format(eType2) + "\n"
                        + " community type-3 " + LogFinal.format(bType3) + "\t" + LogFinal.format(eType3)
                , 3);
    }



    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        List<String> dataSetList = new ArrayList<String>();
//        dataSetList.add("music");
        dataSetList.add("tmdb");
//        dataSetList.add("smallimdb");
//        dataSetList.add("DBPedia");
//        dataSetList.add("DBLPWithWeight");

        int[] kArry = new int[]{5};

        for (String dataSetName : dataSetList) {
            String graphDataSetPath = Config.root + "/" + dataSetName;
            String metaPathsPath = Config.root + "/" + dataSetName;
            avgInfValue vdTest = new avgInfValue(graphDataSetPath, metaPathsPath, dataSetName);
            for (int k : kArry) {
                vdTest.testAll(k);
//                vdTest.compareICS();
            }
            LogFinal.log("\n", 2);
            LogPart.log("\n");
        }
    }


}