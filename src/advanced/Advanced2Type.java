package advanced;

import basic.Comm2Type;
import build.*;
import javafx.util.Pair;
import util.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Advanced2Type implements Comm2Type {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private List<Integer> sortedIndex = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap = new HashMap<Integer, Set<Integer>>();
    Sort sort = new Sort();
    deepCopy deepCopy = new deepCopy();
    public Map<Edge, Integer> edge2Weight = new HashMap<Edge, Integer>();
    public Map<Integer, DualHeapNon> pnb2WeightHeap = new HashMap<>();

    int res_cnt = 0;

    long buildMaptime = 0;
    long deepcopyTime = 0;
    long allVist = 0;
    long modifCnt = 0;
    long total = 0;
    public ArrayList<Double> sortedWeight = new ArrayList<>();
    double realVal = 0.0;
    double upperVal = 0.0;

    public Advanced2Type(int graph[][], int vertexType[], int edgeType[], double weight[],
                         int queryK, MetaPath queryMPath) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
        this.weight = weight;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        this.sortedIndex = sort.sortedIndex(weight);
        for (double value : weight) {
            sortedWeight.add(value);
        }
    }


    public Map<double[], Set<Integer>> computeComm(String datasetName) throws ExecutionException, InterruptedException {

        FastBCore BKCore = new FastBCore(graph, vertexType, edgeType);
        Set<Set<Integer>> conSet = BKCore.query(queryMPath, queryK);
        Set<Integer> keepSet = new HashSet<Integer>(); // contain all type nodes. (eg : authors + papers)
        for (Set<Integer> subSet : conSet) {
            keepSet.addAll(subSet);
        }
        System.out.println(conSet.size());
        System.out.println(keepSet.size());
        HomoGraphBuilderImprved GP = new HomoGraphBuilderImprved(graph, vertexType, edgeType, queryMPath, weight);
        Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
        int minVer = 0;

        for (Set<Integer> type1 : conSet) {
            BatchSearchImproved association = GP.buildNonSort(type1);

            pnbMap = association.pnbMap;
            edge2Weight = association.edge2Weight;
            Set<Integer> type2 = association.type2;
            pnb2WeightHeap = association.skylinePathMap;
            Set<Integer> sortedType1 = new LinkedHashSet<Integer>();
            List<Integer> sortedType2 = new ArrayList<Integer>();
            for (Integer v : sortedIndex) {
                if (vertexType[v] == queryMPath.getEndType() && type1.contains(v)) {
                    sortedType1.add(v);
                }
                if (vertexType[v] == queryMPath.getSecondType() && type2.contains(v)) {
                    sortedType2.add(v);
                }

            }

            tmpMap = deepCopy.copyMap(pnbMap);
            ArrayList<Integer> cvs = new ArrayList<Integer>();
            Deque<Integer> keys = new ArrayDeque<Integer>();
            ArrayList<Pair<Integer, Double>> conditionInf = new ArrayList<Pair<Integer, Double>>();
            PriorityQueue<Double> maxTop = new PriorityQueue<Double>(new Comparator<Double>() {
                @Override
                public int compare(Double o1, Double o2) {
                    return o1 >= o2 ? -1 : 1;

                }
            });

            while (!sortedType1.isEmpty()) {
                minVer = sortedType1.iterator().next();
                keys.add(minVer);
                PriorityQueue<Double> topK = new PriorityQueue<Double>();
                Queue<Integer> queue = new LinkedList<Integer>();
                Set<Integer> minVerSet = tmpMap.get(minVer);
                for (Integer tar : minVerSet) {
                    double kWeight = pnb2WeightHeap.get(tar).getQueryKWeight(queryK);
                    if (topK.size() < queryK) {
                        topK.add(kWeight);
                    } else if (topK.size() == queryK) {
                        if (!topK.isEmpty() && topK.peek() < kWeight) {
                            topK.poll();
                            topK.add(kWeight);
                        }
                    }
                }
                topK.add(pnb2WeightHeap.get(minVer).getQueryKWeight(queryK));
                conditionInf.add(new Pair<>(minVer, topK.peek()));
                maxTop.add(topK.peek());
                queue.offer(minVer);
                while (!queue.isEmpty()) {
                    int v = queue.poll();
                    if (!tmpMap.containsKey(v)) {
                        continue;
                    }
                    for (int i : tmpMap.get(v)) {
                        Set<Integer> pnb = tmpMap.get(i);
                        if (pnb.size() == queryK) {
                            queue.offer(i);
                        }
                        pnb2WeightHeap.get(i).remove(v, edge2Weight.get(new Edge(v, i)));

                        pnb.remove(v);
                    }
                    tmpMap.remove(v);
                    pnb2WeightHeap.remove(v);
                    cvs.add(v);
                    sortedType1.remove(v);
                }
            }
            long ee = System.currentTimeMillis();
            tmpMap.clear();
            double last_f2 = -1 * Double.MIN_VALUE;
            int len = conditionInf.size();
            total += len;
            Map<Double, Map<Double, Set<Integer>>> conditonResult = new HashMap<>();
            for (int i = len - 1; i >= 0; i--) {
                // early stop to speed up
                if (maxTop.peek() <= last_f2) {
                    break;
                }
                int u = conditionInf.get(i).getKey();
                double f2_val = conditionInf.get(i).getValue();
                double upper = f2_val;
                keys.pollLast();

                int indexU = cvs.indexOf(u);
                maxTop.remove(f2_val);
                if (f2_val <= last_f2) {
                    continue;
                }
                long sbuild = System.currentTimeMillis();
                Set<int[]> conditionSet = new HashSet<>();

                buildMap(indexU, cvs, tmpMap, f2_val, last_f2, keys, conditionSet);
                allVist += 1;
                long ebuild = System.currentTimeMillis();
                buildMaptime += (ebuild - sbuild);
                Set<Integer> community = new HashSet<Integer>();
                double f1 = weight[u];
                System.out.println("f1 = " + f1);
                findCommunity(community, u);
                if (community.isEmpty()) {
                    modifCnt += 1;
                    List<Double> sortedType2Weight = new ArrayList<>();
                    for (int edge[] : conditionSet) {
                        sortedType2Weight.add(getWeight(edge[0], edge[1]));
                    }
                    sortedType2Weight.sort(new myComparator<Double>(weight));
                    int l = 0;
                    int r = sortedType2Weight.size() - 1;
                    // in order
                    while (community.isEmpty() && r >= l) {
                        f2_val = sortedType2Weight.get(r);
                        Map<Integer, Set<Integer>> findMap = deepCopy.copyMap(tmpMap);

                        addEdgeAndfindCommunity(community, findMap, f2_val, u, conditionSet, true);
                        r = Math.max(0, r);
                        f2_val = sortedType2Weight.get(r);
                        r -= 1;
                    }

                    System.out.printf("new f2 is %f: \n", f2_val);
                }
                if (f2_val <= last_f2) {
                    continue;
                } else {
                    addEdgeAndfindCommunity(community, tmpMap, f2_val, u, conditionSet, true);
                }
                last_f2 = f2_val;
                conditonResult.put(weight[u], new HashMap<Double, Set<Integer>>());
                upperVal += upper;
                realVal += f2_val;
                System.out.println("f2 = " + f2_val);
                if (conditonResult.get(f1).keySet().isEmpty()) {
                    conditonResult.get(f1).put(f2_val, community);
                } else {
                    Double f2 = conditonResult.get(f1).keySet().iterator().next();
                    if (f2 < f2_val) {
                        conditonResult.get(f1).remove(f2);
                        conditonResult.get(f1).put(f2_val, community);
                    }
                }

            }
            for (Double f1 : conditonResult.keySet()) {
                Double f2 = conditonResult.get(f1).keySet().iterator().next();
                res_cnt++;
                result.put(new double[]{f1, f2}, conditonResult.get(f1).get(f2));
            }
        }

        String logInfo = datasetName + "\t" + queryMPath.toString() + "\t" + "queryK : " + queryK + "\t" + "modify  : " + modifCnt + "\t" + "allVisted : " + allVist + "\t" + "total:" + total;
//        LogAna2.log(logInfo);
        result = filterComm(result);
        return result;
    }

    private Map<double[], Set<Integer>> filterComm(Map<double[], Set<Integer>> Communities) {
        Map<double[], Set<Integer>> res = new HashMap<>();

        List<Double> type1List = new ArrayList<>();
        Map<Double, Double> f12f2 = new HashMap<>();
        Map<Pair<Double, Double>, Set<Integer>> mapComm = new HashMap<>();
        for (Map.Entry<double[], Set<Integer>> entries : Communities.entrySet()) {
            type1List.add(entries.getKey()[0]);
            if (f12f2.containsKey(entries.getKey()[0])) {
                double tmp = entries.getKey()[1];
                if (f12f2.get(entries.getKey()[0]) < tmp) {
                    f12f2.replace(entries.getKey()[0], entries.getKey()[1]);
                }
            } else {
                f12f2.put(entries.getKey()[0], entries.getKey()[1]);
            }
            mapComm.put(new Pair<>(entries.getKey()[0], entries.getKey()[1]), entries.getValue());
        }
        int cnt = 0;
        if (type1List.size() <= 1) {
            return Communities;
        }
        type1List.sort(new myComparator(weight));
        Double f_1 = type1List.get(type1List.size() - 1);
        Double f_2 = f12f2.get(f_1);
        res.put(new double[]{f_1, f_2}, mapComm.get(new Pair<>(f_1, f_2)));
        for (int i = type1List.size() - 2; i >= 0; i--) {
            Double new_f1 = type1List.get(i);

            Double new_f2 = f12f2.get(new_f1);
            if (new_f1 == 8.0) {
                System.out.println("new f_2 is :" + new_f2);
            }
            if (new_f2 > f_2) {
                f_2 = new_f2;
                res.put(new double[]{new_f1, new_f2}, mapComm.get(new Pair<>(new_f1, new_f2)));
            }
        }

        return res;
    }



    private Set<Integer> addEdgeAndfindCommunity(Set<Integer> community, Map<Integer, Set<Integer>> tmpMap, double upperValue, int keyID, Set<int[]> conditionAddSet, boolean isDeepCopy) {
        //1. add edge
        addEdge(tmpMap, upperValue, conditionAddSet);
        //2. find community
        community.clear();
        community.addAll(tmpMap.keySet());
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();
        Map<Integer, Set<Integer>> localMap = new HashMap<>();
        if (isDeepCopy) {
            long s = System.currentTimeMillis();
            localMap = deepCopy.copyMap(tmpMap);
            long e = System.currentTimeMillis();
            deepcopyTime += (e - s);
        } else {
            localMap = tmpMap;
        }
        for (Map.Entry<Integer, Set<Integer>> entry : localMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyID) {
                community.clear();
                return deleteSet;
            }
            community.remove(curId);
            Set<Integer> pnbSet = localMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = localMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
        return deleteSet;
    }

    private void addEdge(Map<Integer, Set<Integer>> localMap, double upperValue, Set<int[]> conditionAddSet) {
        for (int[] edge : conditionAddSet) {
            int key = edge[0];
            int nb = edge[1];
            if (getWeight(key, nb) >= upperValue) {
                if (localMap.containsKey(key)) {
                    localMap.get(key).add(nb);
                } else {

                    Set<Integer> tmpSet = new HashSet<>();
                    tmpSet.add(nb);
                    localMap.put(key, tmpSet);
                }
                if (localMap.containsKey(nb)) {
                    localMap.get(nb).add(key);
                } else {
                    Set<Integer> tt = new HashSet<>();
                    tt.add(key);
                    localMap.put(nb, tt);

                }

            }
        }

    }


    private void buildMap(int indexU, ArrayList<Integer> cvs, Map<Integer, Set<Integer>> tmpMap, double f2_val, double last_f2, Deque<Integer> keys, Set<int[]> conditionSet) {
        for (int j = indexU; j < cvs.size(); j++) {
            int v = cvs.get(j);
            if (j != indexU && keys.contains(v)) {
                break;
            }
            Set<Integer> nbSet = pnbMap.get(v);
            Set<Integer> tmpSet = new HashSet<Integer>();
            for (int nb : nbSet) {
                if (!tmpMap.containsKey(nb)) {
                    continue;
                }

                if (edge2Weight.containsKey(new Edge(v, nb)) && getWeight(v, nb) >= f2_val) {
                    tmpMap.get(nb).add(v);
                    tmpSet.add(nb);
                } else if (getWeight(v, nb) >= last_f2) {
                    conditionSet.add(new int[]{v, nb});
                }
            }
            tmpMap.put(v, tmpSet);
        }
    }

    private double getWeight(int key, int val) {
        Edge edge = new Edge(key, val);

        return sortedWeight.get(edge2Weight.get(edge));
    }

    public void findCommunity(Set<Integer> community, int keyId) {
        Map<Integer, Set<Integer>> localMap = new HashMap<Integer, Set<Integer>>();
        long s = System.currentTimeMillis();
        localMap = deepCopy.copyMap(tmpMap);
        long e = System.currentTimeMillis();

        deepcopyTime += (e - s);
        community.addAll(localMap.keySet());
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();

        for (Map.Entry<Integer, Set<Integer>> entry : localMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) { //iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            if (curId == keyId) {
                community.clear();
                return;
            }
            community.remove(curId);
            Set<Integer> pnbSet = localMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = localMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        queue.add(pnbId);
                        deleteSet.add(pnbId);
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        //        int vertex1[] = {0, 3, 0}, edge1[] = {2, 5};

        //1-1-0-0-1
//        String metaPath = "75-86-422-749-75";
        String metaPath_str = "0-6-4-7-0";
//        String metaPath = "1-1-0-0-1";
        MetaPath metaPath1 = new MetaPath(metaPath_str);
//        DataReader dataReader = new DataReader(Config.musicGraph, Config.musicVertex, Config.musicEdge, Config.musicWeight);
        DataReader dataReader = new DataReader(Config.tmdbGraph, Config.tmdbVertex, Config.tmdbEdge, Config.tmdbWeight);
//        DataReader dataReader = new DataReader(Config.FreeBaseGraph, Config.FreeBaseVertex, Config.FreeBaseEdge, "");

//        DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
//        DataReader dataReader = new DataReader(Config.musicGraph, Config.musicVertex, Config.musicEdge, Config.musicWeight);
//        DataReader dataReader = new DataReader(Config.dbpediaGraph, Config.dbpediaVertex, Config.dbpediaEdge, Config.dbpediaWeight);
//        DataReader dataReader = new DataReader(Config.tmdbGraph, Config.tmdbVertex, Config.tmdbEdge, Config.tmdbWeight);

        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
        double weight[] = dataReader.readWeight();

        Advanced2Type InfCommunities = new Advanced2Type(graph, vertexType, edgeType, weight, 5, metaPath1);
        Map<double[], Set<Integer>> Communities = InfCommunities.computeComm("");
        Communities.forEach((key, value) -> {
            System.out.println(Arrays.toString(key) + "    " + value);
        });
        long endTime = System.currentTimeMillis();
        System.out.println("res cnt is 🎉 : " + Communities.size());
        System.out.println("The total running time is：" + (endTime - startTime) + "ms");
    }
}