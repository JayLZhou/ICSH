package exp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SmallGraph {
    public Map<Integer, Integer> newVidMap;
    public Map<Integer, Integer> oldVidMap;
    public Map<Integer, Integer> newEidMap;

    public int[][] smallGraph;
    public int[] smallGraphVertexType;
    public int[] smallGraphEdgeType;
    public double[] smallGraphWeight;

    private int[][] graph;
    private int[] vertexType;
    private int[] edgeType;
    private double[] weight;

    public SmallGraph(int[][] graph, int[] vertexType, int[] edgeType, double[] weight) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
        this.weight = weight;
    }

    private void getNewVidMap(int part, int total) {
        int smallGraphSize = (graph.length * part) / total;
        System.out.println(smallGraphSize);
        newVidMap = new HashMap<Integer, Integer>();// oldVid to newVid
        int newVid = 0;
        for (int i = 0; i < graph.length; i = i + total) {
            for (int j = 0; j < part; j++) {
                int oldVid = i + j;
                if (!newVidMap.containsKey(oldVid)) {
                    newVidMap.put(oldVid, newVid);
                    newVid++;
                    if (newVidMap.size() >= smallGraphSize) {
                        return;
                    }
                }
            }
        }

    }

    private void getSmallGraphVertexType() {
        smallGraphVertexType = new int[newVidMap.size()];
        for (int oldVid : newVidMap.keySet()) {
            int newVid = newVidMap.get(oldVid);
            smallGraphVertexType[newVid] = vertexType[oldVid];
        }
    }

    private void getSmallGraphWeight() {
        smallGraphWeight = new double[newVidMap.size()];
        for (int oldVid : newVidMap.keySet()) {
            int newVid = newVidMap.get(oldVid);
            smallGraphWeight[newVid] = weight[oldVid];
        }
    }

    public void getSmallGraph(int part, int total) {
        getNewVidMap(part, total);
        getSmallGraphVertexType();
        getSmallGraphWeight();
        newEidMap = new HashMap<Integer, Integer>();
        this.smallGraph = new int[newVidMap.size()][];
        int newEid = 0;
        for (int oldVid : newVidMap.keySet()) {
            int newVid = newVidMap.get(oldVid);
            int numOfNeighbor = 0;
            for (int i = 0; i < graph[oldVid].length; i = i + 2) {
                if (newVidMap.containsKey(graph[oldVid][i])) {
                    numOfNeighbor++;
                }
            }
            smallGraph[newVid] = new int[2 * numOfNeighbor];
            int location = 0;
            for (int i = 0; i < graph[oldVid].length; i = i + 2) {
                if (newVidMap.containsKey(graph[oldVid][i])) {
                    int neighborNid = newVidMap.get(graph[oldVid][i]);
                    int neighborEid = graph[oldVid][i + 1];
                    smallGraph[newVid][location] = neighborNid;
                    smallGraph[newVid][location + 1] = newEid;
                    newEidMap.put(neighborEid, newEid);
                    newEid++;
                    location = location + 2;
                }
            }
        }
        smallGraphEdgeType = new int[newEid];
        for (int i = 0; i < edgeType.length; i++) {
            int type = edgeType[i];
            if (newEidMap.containsKey(i)) {
                int newEdgeId = newEidMap.get(i);
                smallGraphEdgeType[newEdgeId] = type;
            }
        }
    }

    private void getNewVidMap(int part, int total,
                              Set<Integer> nodesSet/* these nodes are needed in the final graph */) {
        int smallGraphSize = (graph.length * 14) / total;
        System.out.println("Size : " + smallGraphSize);
        newVidMap = new HashMap<Integer, Integer>();// oldVid to newVid
        oldVidMap = new HashMap<Integer, Integer>(); // newVid to oldVid
        int newVid = 0;
        for (int nodeId : nodesSet) {
            if (!newVidMap.containsKey(nodeId)) {
                newVidMap.put(nodeId, newVid);
                oldVidMap.put(newVid, nodeId);
                newVid++;
                if (newVidMap.size() >= smallGraphSize) {
                    return;
                }
            }
        }

        for (int i = 0; i < graph.length; i = i + total) {
            for (int j = 0; j < 21; j++) {
                int oldVid = i + j;
                Set<Integer> deleteTop = new HashSet<>();

                for (int a = 0; a < 350; a++) {
                    deleteTop.add(a + 409340);
                }

                deleteTop.add(21715 + 409340);
                deleteTop.add(12181 + 409340);
                deleteTop.add(19499 + 409340);
                deleteTop.add(24958 + 409340);
                deleteTop.add(447 + 409340);
                deleteTop.add(34186 + 409340);
                int topicOff = 815020;
//                deleteTop.add(0 + topicOff);
                deleteTop.add(1 + topicOff);
                deleteTop.add(3 + topicOff);
//                deleteTop.add(22 + topicOff);
//                deleteTop.add(22 + topicOff);
                if (deleteTop.contains(oldVid)) {
                    continue;
                }
                if (!newVidMap.containsKey(oldVid)) {
                    newVidMap.put(oldVid, newVid);
                    oldVidMap.put(newVid, oldVid);
                    newVid++;
                    if (newVidMap.size() >= smallGraphSize) {
                        return;
                    }
                }
            }
        }

    }

    public void getSmallGraph(int part, int total, Set<Integer> nodesSet) {
        getNewVidMap(part, total, nodesSet);
        getSmallGraphVertexType();
        getSmallGraphWeight();
        newEidMap = new HashMap<Integer, Integer>();
        this.smallGraph = new int[newVidMap.size()][];
        int newEid = 0;
        for (int oldVid : newVidMap.keySet()) {
            int newVid = newVidMap.get(oldVid);
            int numOfNeighbor = 0;
            for (int i = 0; i < graph[oldVid].length; i = i + 2) {
                if (newVidMap.containsKey(graph[oldVid][i])) {
                    numOfNeighbor++;
                }
            }
            smallGraph[newVid] = new int[2 * numOfNeighbor];
            int location = 0;
            for (int i = 0; i < graph[oldVid].length; i = i + 2) {
                if (newVidMap.containsKey(graph[oldVid][i])) {
                    int neighborNid = newVidMap.get(graph[oldVid][i]);
                    int neighborEid = graph[oldVid][i + 1];
                    smallGraph[newVid][location] = neighborNid;
                    smallGraph[newVid][location + 1] = newEid;
                    newEidMap.put(neighborEid, newEid);
                    newEid++;
                    location = location + 2;
                }
            }
        }
        smallGraphEdgeType = new int[newEid];
        for (int i = 0; i < edgeType.length; i++) {
            int type = edgeType[i];
            if (newEidMap.containsKey(i)) {
                int newEdgeId = newEidMap.get(i);
                smallGraphEdgeType[newEdgeId] = type;
            }
        }
        System.out.println("vertex size is : " + newVidMap.size());
        System.out.println("edge size is : " + newEidMap.size());
    }
}
