package util;

import javafx.util.Pair;

import java.util.*;

class MetaPathGenerater {
    int schemaGraph[][];
    public Map<Integer, Set<Integer>> type2Id = new HashMap<>(); // eg : 0 -> {1,3, ... } 所有type为0的点
    private int graph[][] = null;// data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;// vertex -> type
    private int edgeType[] = null;

    public MetaPathGenerater(int graph[][], int [] vertexType, int[] edgeType) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
    }

    public ArrayList<MetaPath> generateHalfMetaPath(int l, int targetType) {
        ArrayList<MetaPath> S = new ArrayList<MetaPath>();
        ArrayList<MetaPath> X = new ArrayList<MetaPath>();
        HashMap<Integer, Integer> edgeTypeMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < schemaGraph.length; i++) {
            if (schemaGraph[i] == null) {
                continue;
            }
            for (int j = 0; j < schemaGraph[i].length; j++) {
                if (j % 2 == 0) {
                    for (int k = 0; k < schemaGraph[schemaGraph[i][j]].length; k++) {
                        if (k % 2 == 0) {
                            if (schemaGraph[schemaGraph[i][j]][k] == i) {
                                edgeTypeMap.put(schemaGraph[i][j + 1], schemaGraph[schemaGraph[i][j]][k + 1]);
                            }
                        }
                    }
                }
            }
        }
        int metaPathEdges[] = new int[l];
        int metaPathVertices[] = new int[l + 1];
        metaPathVertices[0] = targetType;
        S.add(new MetaPath(metaPathVertices, metaPathEdges));
        for (int i = 0; i < l / 2; i++) {
            ArrayList<MetaPath> sTemp = new ArrayList<MetaPath>();
            for (int j = 0; j < S.size(); j++) {
                MetaPath mp = S.get(j);
                int lastNode = (mp.getVertex())[i];
                for (int k = 0; k < schemaGraph[lastNode].length; k++) {
                    if (k % 2 == 0 && schemaGraph[lastNode][k] != targetType) { // ignore the meta-path like 'APAPAPA'
                        // if (k % 2 == 0){ // not ignore the meta-path like 'APAPAPA'
                        MetaPath mpTemp = new MetaPath(mp);
                        mpTemp.addVertexToPath(i + 1, schemaGraph[lastNode][k]);
                        mpTemp.addEdgeToPath(i, schemaGraph[lastNode][k + 1]);
                        sTemp.add(mpTemp);
                    }
                }
            }
            for (int j = 0; j < sTemp.size(); j++) {
                MetaPath mp = new MetaPath(sTemp.get(j));
                mp.symmetricPath(i + 1, edgeTypeMap);
                X.add(mp);
            }
            S = sTemp;
        }
        return X;
    }
    public  int[][] getSchemaGraph() {
        for (int i = 0; i < edgeType.length; i++) {
            if (type2Id.containsKey(edgeType[i])) {
                type2Id.get(edgeType[i]).add(i);
            } else {
                Set<Integer> tmpSet = new HashSet<>();
                tmpSet.add(i);
                type2Id.put(edgeType[i], tmpSet);
            }
        }
        Set<Pair<Integer, Integer>> uniquePair = new HashSet<>();
        schemaGraph = new int[984][];
        for (int type : type2Id.keySet()) {
            for (int curId : type2Id.get(type)) {
                int [] nb = graph[curId];
                for (int i = 0; i < nb.length; i += 2) {
                    int nbVertexID = nb[i], nbEdgeID = nb[i + 1];
                    uniquePair.add(new Pair<>(vertexType[nbVertexID], edgeType[nbEdgeID]));
                }
            }
            schemaGraph[type] = new int[uniquePair.size() * 2];
            for (int i = 0; i < uniquePair.size() * 2; i += 2) {
                Pair <Integer, Integer> pair = uniquePair.iterator().next();
                schemaGraph[type][i] = pair.getKey();
                schemaGraph[type][i + 1] = pair.getValue();
                uniquePair.remove(pair);
            }

        }
        return schemaGraph;
    }


    public static void main(String[] args) {

        DataReader dataReader = new DataReader(Config.FreeBaseGraph, Config.FreeBaseVertex, Config.FreeBaseEdge, Config.FreeBaseWeight);
//            DataReader dataReader = new DataReader(Config.IMDBGraph, Config.IMDBVertex, Config.IMDBEdge, Config.IMDBWeight);

//            DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
        int graph[][] = dataReader.readGraph();
        int vertexType[] = dataReader.readVertexType();
        int edgeType[] = dataReader.readEdgeType();
//        double weight[] = dataReader.readWeight();
        MetaPathGenerater metaPathGenerater = new MetaPathGenerater(graph, vertexType,edgeType );
        int[][] schemaGraph = metaPathGenerater.getSchemaGraph();
    }


}
