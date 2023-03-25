package build;

import util.DualHeapNon;
import util.MetaPath;

import javax.management.openmbean.TabularData;
import java.util.*;

class maxComparator implements Comparator<Integer> {
    private double[] weight = null;

    public maxComparator(double[] weight) {
        this.weight = weight;
    }


    public int compare(Integer a, Integer b) {
        double weightS1 = weight[a];
        double weightS2 = weight[b];
        return weightS1 >= weightS2 ? -1 : 1;
    }
}

public class BatchSearchImproved {
    private int graph[][] = null;// data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;// vertex -> type
    private int edgeType[] = null;// edge -> type
    private MetaPath queryMPath = null;
    private double weight[] = null;
    public Map<Edge, Integer> edge2Weight = new HashMap<Edge, Integer>();
    public HashMap<Edge, int[][]> edgeAssoType3 = new HashMap<Edge, int[][]>(800000000);
    public HashMap<Edge, Map<Integer, int[][]>> edgeAssoTypeH = new HashMap<Edge, Map<Integer, int[][]>>(); // edge -> v
    public HashMap<Integer, Set<Edge>> midTypeAssoEdge = new HashMap<Integer, Set<Edge>>(); // v -> edges
    public HashMap<Integer, int[][]> midAssoType3 = new HashMap<Integer, int[][]>(); // v-> (p,t)

    public HashMap<Integer, Set<int[]>> midLeftPairMap = new HashMap<Integer, Set<int[]>>();
    public Map<Integer, Set<Integer>> pnbMap = new HashMap<Integer, Set<Integer>>();
    public Set<Integer> type3 = new HashSet<Integer>();
    public Set<Integer> type2 = new HashSet<Integer>();
    public List<Set<Integer>> typeList = new LinkedList<>();
    public Map<Edge, Set<Integer>> edge2Paper = new HashMap<Edge, Set<Integer>>();
    public Map<Integer, DualHeapNon> skylinePathMap = new HashMap<>();

    public BatchSearchImproved(int graph[][], int vertexType[], int edgeType[], MetaPath queryMPath, double[] weight) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
        this.queryMPath = queryMPath;
        this.weight = weight;
    }

    public void collect2Type(int startId, Set<Integer> keepSet) {
        Set<Integer> anchorSet = new HashSet<Integer>();
        Set<Integer> nbSet = new HashSet<Integer>();

        int targetVType = queryMPath.vertex[1], targetEType = queryMPath.edge[0];
        int nb[] = graph[startId];
        for (int i = 0; i < nb.length; i += 2) {
            int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
            if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                anchorSet.add(nbVertexID);
            }
        }

        targetVType = queryMPath.vertex[2];
        targetEType = queryMPath.edge[1];
        for (int anchorId : anchorSet) {
            nb = graph[anchorId];
            for (int i = 0; i < nb.length; i += 2) {
                int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                    if (keepSet.contains(nbVertexID) && startId != nbVertexID) {
                        nbSet.add(nbVertexID);
                        if (startId > nbVertexID) {
                            continue;
                        }
                        type2.add(anchorId);
                        Edge endpts = new Edge(startId, nbVertexID);
                        if (!edge2Weight.containsKey(endpts)) {
                            edge2Weight.put(endpts, anchorId);
                        } else {
                            double edgeWeight = weight[edge2Weight.get(endpts)];
                            if (edgeWeight < weight[anchorId]) {
                                edge2Weight.replace(endpts, anchorId);
                            }
                        }

                    }
                }
            }

        }
        pnbMap.put(startId, nbSet);
    }

    public void collect3Type(int startId, Set<Integer> keepSet, boolean isOver) {
        int midLayer = queryMPath.pathLen >> 1;
        Set<Integer> anchorSet = new HashSet<Integer>();
        HashMap<Integer, Integer> leftMidMap = new HashMap<>(); // map t -> left's max p
        HashMap<Integer, Set<Integer>> rightMidMap = new HashMap<>(); // eg : p --> t
        HashMap<Integer, Set<Integer>> endMap = new HashMap<>(); // eg : p --> t
        anchorSet.add(startId);
        for (int layer = 0; layer < queryMPath.pathLen; layer++) {
            int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];
            Set<Integer> nextAnchorSet = new HashSet<Integer>();
            for (int anchorId : anchorSet) {
                int[] nb = graph[anchorId];
                for (int i = 0; i < nb.length; i += 2) {
                    int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                    if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                        if (layer < queryMPath.pathLen - 1) {
                            nextAnchorSet.add(nbVertexID);
                            if (layer == midLayer - 1) {
                                if (leftMidMap.containsKey(nbVertexID)) {
                                    int pastVertexID = leftMidMap.get(nbVertexID);
                                    if (weight[anchorId] > weight[pastVertexID]) {
                                        leftMidMap.replace(nbVertexID, anchorId);
                                    }
                                } else {
                                    leftMidMap.put(nbVertexID, anchorId);
                                }
                            }
                            if (layer == midLayer) {
                                if (rightMidMap.containsKey(nbVertexID)) {
                                    rightMidMap.get(nbVertexID).add(anchorId);
                                } else {
                                    Set<Integer> tmpSet = new HashSet<>();
                                    tmpSet.add(anchorId);
                                    rightMidMap.put(nbVertexID, tmpSet);
                                }
                            }
                        } else {
                            if (keepSet.contains(nbVertexID) && startId != nbVertexID) { //impose restriction
                                nextAnchorSet.add(nbVertexID);
                                if (startId < nbVertexID) {
                                    if (endMap.containsKey(nbVertexID)) {
                                        endMap.get(nbVertexID).add(anchorId); // a --> p
                                    } else {
                                        Set<Integer> tmpSet = new HashSet<>();
                                        tmpSet.add(anchorId);
                                        endMap.put(nbVertexID, tmpSet);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            anchorSet = nextAnchorSet;
        }
        pnbMap.put(startId, anchorSet);
        for (int pnb : anchorSet) {
            if (startId > pnb) {
                continue;
            }
            Set<int[]> alivePair = new HashSet<>();
            List<int[]> aliveList = new ArrayList<>();
            Set<Integer> type2Set = endMap.get(pnb);
            for (int type2 : type2Set) {
                Set<Integer> type3Set = rightMidMap.get(type2);
                for (int type3 : type3Set) {
                    int domainType2 = type2;
                    if (weight[domainType2] > weight[leftMidMap.get(type3)]) {
                        domainType2 = leftMidMap.get(type3);
                    }
                    int[] newKey = new int[]{domainType2, type3};
                    aliveList.add(newKey);
                }
            }
            alivePair = filter(aliveList);
            for (int[] pair : alivePair) {
                type2.add(pair[0]);
                type3.add(pair[1]);
            }

            int[][] aliveArray = new int[alivePair.size()][2];
            int cnt = 0;
            for (int[] pair : alivePair) {
                aliveArray[cnt][0] = pair[0];
                aliveArray[cnt][1] = pair[1];
                cnt += 1;
            }
            edgeAssoType3.put(new Edge(new int[]{startId, pnb}), aliveArray);
        }

    }

    public void collect2TypeNonSort(int startId, Set<Integer> keepSet, boolean b) {
        Set<Integer> anchorSet = new HashSet<Integer>();
        Set<Integer> nbSet = new HashSet<Integer>();
        Map<Integer, Set<Integer>> tmpAsso = new HashMap<Integer, Set<Integer>>(); // recorded p-> connects A
        Map<Integer, Set<Integer>> verAsso = new HashMap<Integer, Set<Integer>>();

        int targetVType = queryMPath.vertex[1], targetEType = queryMPath.edge[0];
        int nb[] = graph[startId];
        for (int i = 0; i < nb.length; i += 2) {
            int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
            if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                anchorSet.add(nbVertexID);
            }
        }
        targetVType = queryMPath.vertex[2];
        targetEType = queryMPath.edge[1];
        Set<Integer> removePaper = new HashSet<>();
        for (int anchorId : anchorSet) {
            nb = graph[anchorId];
            Set<Integer> tmpSet = new HashSet<Integer>();
            for (int i = 0; i < nb.length; i += 2) {
                int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                    if (keepSet.contains(nbVertexID) && startId != nbVertexID) {
                        nbSet.add(nbVertexID);
                        tmpSet.add(nbVertexID);
                        if (startId > nbVertexID) {
                            Edge endpts = new Edge(startId, nbVertexID);
                            if (edge2Weight.containsKey(endpts)) {
                                // if already confirmed the maximal vertex between startId and nbVertexID
                                int id = edge2Weight.get(endpts);
                                if (tmpAsso.containsKey(id)) {
                                    tmpAsso.get(id).add(nbVertexID);
                                } else {
                                    Set<Integer> tmp = new HashSet<>();
                                    tmp.add(nbVertexID);
                                    tmpAsso.put(id, tmp);
                                }
                            }
                            continue;
                        }
                        Edge endpts = new Edge(startId, nbVertexID);
                        if (!edge2Paper.containsKey(endpts)) {
                            HashSet<Integer> objects = new HashSet<>();
                            objects.add(anchorId);
                            edge2Paper.put(endpts, objects);
                        } else {
                            edge2Paper.get(endpts).add(anchorId);
                        }
                        if (!edge2Weight.containsKey(endpts)) {
                            edge2Weight.put(endpts, anchorId);

                        } else {
                            int oldId = edge2Weight.get(endpts);
                            double edgeWeight = weight[oldId];
                            if (edgeWeight < weight[anchorId]) {
                                edge2Weight.replace(endpts, anchorId);
                                tmpAsso.get(oldId).remove(nbVertexID);
                                if (tmpAsso.get(oldId).size() == 0) {
                                    tmpAsso.remove(oldId);
                                }
                            }
                        }


                    }
                }
            }
            if (!tmpSet.isEmpty()) {
                tmpAsso.put(anchorId, tmpSet);
            }
        }

        pnbMap.put(startId, nbSet);
        type2.addAll(tmpAsso.keySet());

        ArrayList<Integer> keyList = new ArrayList<>(tmpAsso.keySet());
        keyList.sort(new maxComparator(weight));
        for (int i = 0; i < keyList.size(); i++) {
            verAsso.put(keyList.get(i), tmpAsso.get(keyList.get(i)));
//            cnt += tmpAsso.get(keyList.get(i)).size();
        }

        skylinePathMap.put(startId, new DualHeapNon(keyList, verAsso, weight));
    }

    public void collectHType(int startId, Set<Integer> keepSet, boolean isOver) {
        Set<Integer> anchorSet = new HashSet<Integer>();
        anchorSet.add(startId);
        for (int i = 0; i < (queryMPath.pathLen + 2) / 2 - 1; i++) {
            typeList.add(new HashSet<>());
        }
        for (int layer = 0; layer < queryMPath.pathLen; layer++) {
            int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];
            // Example: A-P-T-P-A
            // layer 0: P
            // layer 1: T
            // layer 2: P
            // layer 3 = pathLen - 1: A
            // APVPTPVPA
            // 0 2 4 6 P
            // 1 5 V
            // 3 T
            Set<Integer> nextAnchorSet = new HashSet<Integer>();
            for (int anchorId : anchorSet) {
                int nb[] = graph[anchorId];
                for (int i = 0; i < nb.length; i += 2) {
                    int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                    if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                        if (layer < queryMPath.pathLen - 1) {
                            nextAnchorSet.add(nbVertexID);
                        } else {
                            if (keepSet.contains(nbVertexID)) nextAnchorSet.add(nbVertexID);//impose restriction
                        }
                    }
                }
            }
            if (layer != queryMPath.pathLen - 1) {
                int type = 0;
                if (layer % 2 == 0) {
                    type = 0; // p
                } else if (layer == (queryMPath.pathLen - 2) / 2) {
                    type = 2; // t
                } else {
                    type = 1;
                }
                typeList.get(type).addAll(nextAnchorSet);
            }
            anchorSet = nextAnchorSet;
        }

        anchorSet.remove(startId);
        pnbMap.put(startId, anchorSet);

    }

    public void collectHType_DeleteSet(int startId, Set<Integer> keepSet, Set<Integer> deleteSet, boolean isOver) {
        Set<Integer> anchorSet = new HashSet<Integer>();
        anchorSet.add(startId);
        typeList.clear();
        for (int i = 0; i < (queryMPath.pathLen + 2) / 2 - 1; i++) {
            typeList.add(new HashSet<>());
        }
        int midMidLayer = queryMPath.pathLen >> 2;
        HashMap<Integer, Integer> leftMidMap = new HashMap<>(); // map t -> left's max p
        HashMap<Integer, Set<Integer>> rightMidMap = new HashMap<>(); // eg :  --> t
        HashMap<Integer, Set<Integer>> endMap = new HashMap<>(); // eg : v --> t
        anchorSet.add(startId);
        for (int layer = 0; layer < queryMPath.pathLen >> 1; layer++) {
            int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];
            Set<Integer> nextAnchorSet = new HashSet<Integer>();
            for (int anchorId : anchorSet) {
                int[] nb = graph[anchorId];
                for (int i = 0; i < nb.length; i += 2) {
                    int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                    if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                        if (layer < (queryMPath.pathLen >> 1) - 1) {
                            nextAnchorSet.add(nbVertexID);
                            if (layer == midMidLayer) {
                                if (leftMidMap.containsKey(nbVertexID)) {
                                    int pastVertexID = leftMidMap.get(nbVertexID);
                                    if (weight[anchorId] > weight[pastVertexID]) {
                                        leftMidMap.replace(nbVertexID, anchorId);
                                    }
                                } else {
                                    leftMidMap.put(nbVertexID, anchorId);
                                }
                            }

                        } else {
                            nextAnchorSet.add(nbVertexID);

                            if (endMap.containsKey(nbVertexID)) {
                                endMap.get(nbVertexID).add(anchorId);
                            } else {
                                Set<Integer> tmpSet = new HashSet<>();
                                tmpSet.add(anchorId);
                                endMap.put(nbVertexID, tmpSet);
                            }
                        }
                    }

                }
            }
            anchorSet = nextAnchorSet;
        }
        // collected the half information anchorSet: Topic
        anchorSet.removeAll(deleteSet);
        for (int pnb : anchorSet) {
            Set<int[]> alivePair = new HashSet<>();
            List<int[]> aliveList = new ArrayList<>();
            Set<Integer> type3Set = endMap.get(pnb);
            for (int type3 : type3Set) {
//                Set<Integer> type3Set = rightMidMap.get(type2);
//                for (int type3 : type3Set) {
                int domainType2 = leftMidMap.get(type3);
                int[] newKey = new int[]{domainType2, type3};
                aliveList.add(newKey);

            }
            alivePair = filter(aliveList);
            for (int[] pair : alivePair) {
                type2.add(pair[0]);
                type3.add(pair[1]);
            }

            int[][] aliveArray = new int[alivePair.size()][2];
            int cnt = 0;
            for (int[] pair : alivePair) {
                aliveArray[cnt][0] = pair[0];
                aliveArray[cnt][1] = pair[1];
                cnt += 1;
            }
            midAssoType3.put(pnb, aliveArray);
        }


        Set<Integer> pnbSets = new HashSet<>();
        Set<Integer> startMidSets = new HashSet<>(anchorSet);
        // for each t
        for (
                Integer startMidVertex : startMidSets) {
            anchorSet.clear();
            leftMidMap.clear();
            rightMidMap.clear();
            endMap.clear();
            anchorSet.add(startMidVertex);
            int midLayer = (queryMPath.pathLen >> 1) + (queryMPath.pathLen >> 2);
            for (int layer = queryMPath.pathLen >> 1; layer < queryMPath.pathLen; layer++) {
                int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];
                Set<Integer> nextAnchorSet = new HashSet<Integer>();
                for (int anchorId : anchorSet) {
                    int[] nb = graph[anchorId];
                    for (int i = 0; i < nb.length; i += 2) {
                        int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                        if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                            if (layer < queryMPath.pathLen - 1) {
                                nextAnchorSet.add(nbVertexID);
                                if (layer == midLayer) {
                                    if (leftMidMap.containsKey(nbVertexID)) {
                                        int pastVertexID = leftMidMap.get(nbVertexID);
                                        if (weight[anchorId] > weight[pastVertexID]) {
                                            leftMidMap.replace(nbVertexID, anchorId);
                                        }
                                    } else {
                                        leftMidMap.put(nbVertexID, anchorId);
                                    }
                                }
                            } else {
                                if (keepSet.contains(nbVertexID) && startId != nbVertexID) { //impose restriction
                                    nextAnchorSet.add(nbVertexID);
                                    pnbSets.add(nbVertexID);
                                    if (startId < nbVertexID) {
                                        if (endMap.containsKey(nbVertexID)) {
                                            endMap.get(nbVertexID).add(anchorId); // a --> p
                                        } else {
                                            Set<Integer> tmpSet = new HashSet<>();
                                            tmpSet.add(anchorId);
                                            endMap.put(nbVertexID, tmpSet);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                anchorSet = nextAnchorSet;
            }
            anchorSet.remove(startId);
            for (int pnb : anchorSet) {
                if (startId > pnb) {
                    continue;
                }
                Set<int[]> alivePair = new HashSet<>();
                List<int[]> aliveList = new ArrayList<>();
                Set<Integer> type2Set = endMap.get(pnb);
                for (int type2 : type2Set) {
                    int domainType3 = leftMidMap.get(type2);
                    int[] newKey = new int[]{type2, domainType3};
                    aliveList.add(newKey);
                }

                // There are some important hint:
                // 1. we first obtain the right part pair and filter them.
                // 2. next, we re-filter the (left,right) pair and reserve the dominated part.
//                aliveList.addAll(Arrays.asList(midAssoType3.get(startMidVertex)));
                alivePair = filter(aliveList);
//                Set<int[]> alivePairAll = new HashSet<>());
                List<int[]> aliveAll = new ArrayList<>(Arrays.asList(midAssoType3.get(startMidVertex)));
                aliveAll.addAll(alivePair);
                alivePair.addAll(Arrays.asList(midAssoType3.get(startMidVertex)));
//                alivePairAll.addAll(alivePair);
                Set<int[]> needRemove = filter(aliveAll);
                alivePair.removeAll(needRemove);

                for (int[] pair : alivePair) {
                    type2.add(pair[0]);
                    type3.add(pair[1]);
                }

                int[][] aliveArray = new int[alivePair.size()][2];
                int cnt = 0;
                for (int[] pair : alivePair) {
                    aliveArray[cnt][0] = pair[0];
                    aliveArray[cnt][1] = pair[1];
                    cnt += 1;
                }
                Edge edge = new Edge(new int[]{startId, pnb});
                if (edgeAssoTypeH.containsKey(edge)) {
                    edgeAssoTypeH.get(edge).put(startMidVertex, aliveArray);

                } else {
                    Map<Integer, int[][]> tmpMap = new HashMap<>();
                    tmpMap.put(startMidVertex, aliveArray);
                    edgeAssoTypeH.put(edge, tmpMap);
                }
                if (midTypeAssoEdge.containsKey(startMidVertex)) {

                    midTypeAssoEdge.get(startMidVertex).add(edge);
                } else {
                    Set<Edge> tmpSet = new HashSet<>();
                    tmpSet.add(edge);
                    midTypeAssoEdge.put(startMidVertex, tmpSet);
                }
//                midAssoType3.replace(startMidVertex, aliveArray);
            }
        }
        pnbMap.put(startId, pnbSets);
    }

    class PairCompare implements Comparator<int[]> {
        private double[] weight;

        public PairCompare(double[] weight) {
            this.weight = weight;
        }

        @Override
        public int compare(int[] left, int[] right) {
            if (weight[left[0]] != weight[right[0]]) {
                return weight[left[0]] < weight[right[0]] ? -1 : 1;
            } else {
                return weight[left[1]] < weight[right[1]] ? -1 : 1;

            }
        }

    }

    public Set<int[]> filter(List<int[]> alivePair) {
        alivePair.sort(new PairCompare(weight));
        Set<int[]> validPair = new HashSet<>();
        int length = alivePair.size() - 1;
        validPair.add(alivePair.get(length));
        double type2Limit = weight[alivePair.get(length)[1]];
        for (int i = length - 1; i >= 0; i--) {
            int[] curPair = alivePair.get(i);
            if (weight[curPair[1]] <= type2Limit) {
                continue;
            }
            type2Limit = weight[curPair[1]];
            validPair.add(curPair);
        }
        for (int[] pair : validPair) {
            type2.add(pair[0]);
            type3.add(pair[1]);
        }
        return validPair;
    }
}
