package build;

import java.util.*;

import util.MetaPath;

public class BatchSearch {
    private int graph[][] = null;// data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;// vertex -> type
    private int edgeType[] = null;// edge -> type
    private MetaPath queryMPath = null;
    private double weight[] = null;
    public Map<Edge, Integer> edgeAsso = new HashMap<Edge, Integer>();
    public Map<String, Integer> numOfTT = new HashMap<String, Integer>();
    public Map<String, Integer> numOfST = new HashMap<String, Integer>();
    public Map<Integer, Set<Edge>> verAsso = new HashMap<Integer, Set<Edge>>();
    public Map<Integer, Set<Edge>> FTAndFT = new HashMap<Integer, Set<Edge>>(); // eg : a和a
    public Map<Integer, Set<Integer>> pnbMap = new HashMap<Integer, Set<Integer>>();
    public HashMap<Edge, int[][]> edgeAssoComm3 = new HashMap<Edge, int[][]>();
    public Set<Integer> type2 = new HashSet<Integer>();

    public Set<Integer> type3 = new HashSet<Integer>();

    public BatchSearch(int graph[][], int vertexType[], int edgeType[], MetaPath queryMPath, double [] weight) {
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
            Set<Edge> tmpSet = new HashSet<Edge>();
            if (verAsso.containsKey(anchorId)) {
                tmpSet = verAsso.get(anchorId);
            }
            for (int i = 0; i < nb.length; i += 2) {
                int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                    if (keepSet.contains(nbVertexID) && startId != nbVertexID) {
                        nbSet.add(nbVertexID);
                        if (startId > nbVertexID) {
                            continue;
                        }

                        Edge endpts = new Edge(startId, nbVertexID);
                        tmpSet.add(endpts);
                        if (edgeAsso.containsKey(endpts)) {
                            int count = edgeAsso.get(endpts);
                            count++;

                            edgeAsso.replace(endpts, count);
                        } else {
                            edgeAsso.put(endpts, 1);

                        }
                    }
                }
            }
            if (!tmpSet.isEmpty()) {
                verAsso.put(anchorId, tmpSet);
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
                            if (keepSet.contains(nbVertexID) ) { //impose restriction
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
        anchorSet.remove(startId);
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
            edgeAssoComm3.put(new Edge(new int[]{startId, pnb}), aliveArray);
        }
        if (isOver) {
            for (Edge edge : edgeAssoComm3.keySet()) {
                for (int[] pair : edgeAssoComm3.get(edge)) {
                    int ttKey = pair[1];
                    if (FTAndFT.containsKey(ttKey)) {
                        FTAndFT.get(ttKey).add(edge);
                    } else {
                        Set<Edge> atSet = new HashSet<>();
                        atSet.add(edge);
                        FTAndFT.put(ttKey, atSet);
                    }
                }
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
        return validPair;
    }

    public Set<Integer> collect(int startId, Set<Integer> keepSet) {
        Set<Integer> anchorSet = new HashSet<Integer>();
        anchorSet.add(startId);

        for (int layer = 0; layer < queryMPath.pathLen; layer++) {
            int targetVType = queryMPath.vertex[layer + 1], targetEType = queryMPath.edge[layer];

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
            anchorSet = nextAnchorSet;
        }

        anchorSet.remove(startId);//2018-9-19-bug: remove the duplicated vertex
        return anchorSet;
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
}