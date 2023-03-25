package build;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import util.BatchLinker;
import util.MetaPath;

public class FastBCore {
    private int graph[][] = null;//data graph, including vertex IDs, edge IDs, and their link relationships
    private int vertexType[] = null;//vertex -> type
    private int edgeType[] = null;//edge -> type
    private int queryK = -1;
    private MetaPath queryMPath = null;

    public FastBCore(int graph[][], int vertexType[], int edgeType[]) {
        this.graph = graph;
        this.vertexType = vertexType;
        this.edgeType = edgeType;
    }
    public Set<Integer> query(int queryId, MetaPath queryMPath, int queryK) {
        this.queryK = queryK;
        this.queryMPath = queryMPath;

        //step 1: compute the connected subgraph via batch-search with labeling (BSL)
        BatchLinker batchLinker = new BatchLinker(graph, vertexType, edgeType);
        Set<Integer> keepSet = batchLinker.link(queryId, queryMPath);

        //step 2: initialization
        Map<Integer, Set<Integer>> pnbMap = new HashMap<Integer, Set<Integer>>();//a vertex -> its pnbs
        Map<Integer, List<Set<Integer>>> visitMap = new HashMap<Integer, List<Set<Integer>>>();//a vertex -> its visited vertices

        //step 3: find k-neighbors for each vertex
        for (int startVertex : keepSet) {
            List<Set<Integer>> visitList = new ArrayList<Set<Integer>>();
            for (int i = 0; i <= queryMPath.pathLen; i++) visitList.add(new HashSet<Integer>());
            Set<Integer> nbSet = new HashSet<Integer>();

            findFirstKNeighbors(startVertex, startVertex, 0, visitList, nbSet);//find the first k neighbors
            pnbMap.put(startVertex, nbSet);
            visitMap.put(startVertex, visitList);
        }

        //step 4: compute the k-core
        if (pnbMap.get(queryId).size() < queryK) return null;
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();//mark the delete vertices
        for (Map.Entry<Integer, Set<Integer>> entry : pnbMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) {//iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            keepSet.remove(curId);

            Set<Integer> pnbSet = pnbMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = pnbMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        addMoreNeighbors(pnbId, pnbId, 0, visitMap.get(pnbId), tmpSet, keepSet);
                        if (tmpSet.size() < queryK) {
                            queue.add(pnbId);
                            deleteSet.add(pnbId);
                        }
                    }
                }
            }
        }

        //step 5: find the connected community
        BatchLinkerCSH ccFinder = new BatchLinkerCSH(graph, vertexType, edgeType, queryId, queryMPath, keepSet, pnbMap);
        return ccFinder.computeCC();
    }

    public Set<Set<Integer>> query(MetaPath queryMPath, int queryK) {
        this.queryK = queryK;
        this.queryMPath = queryMPath;

        //step 1: compute the connected subgraph via batch-search with labeling (BSL)
//		BatchLinker batchLinker = new BatchLinker(graph, vertexType, edgeType);
//		Set<Integer> keepSet = batchLinker.link(queryId, queryMPath);

        //step 2: initialization
        Set<Integer> keepSet = new HashSet<Integer>();
        Map<Integer, Set<Integer>> pnbMap = new HashMap<Integer, Set<Integer>>();//a vertex -> its pnbs
        Map<Integer, List<Set<Integer>>> visitMap = new HashMap<Integer, List<Set<Integer>>>();//a vertex -> its visited vertices
        Set<Set<Integer>> conSet = new HashSet<Set<Integer>>();

        for (int v = 0; v < vertexType.length; v++) {
            if (vertexType[v] == queryMPath.getEndType()) {
                keepSet.add(v);
            }
        }

        //step 3: find k-neighbors for each vertex
        for (int startVertex : keepSet) {
            List<Set<Integer>> visitList = new ArrayList<Set<Integer>>();
            for (int i = 0; i <= queryMPath.pathLen; i++) {
                visitList.add(new HashSet<Integer>());
            }
            Set<Integer> nbSet = new HashSet<Integer>();

            findFirstKNeighbors(startVertex, startVertex, 0, visitList, nbSet);//find the first k neighbors
            pnbMap.put(startVertex, nbSet);
            visitMap.put(startVertex, visitList);
        }

        //step 4: compute the k-core
        Queue<Integer> queue = new LinkedList<Integer>();
        Set<Integer> deleteSet = new HashSet<Integer>();//mark the delete vertices
        for (Map.Entry<Integer, Set<Integer>> entry : pnbMap.entrySet()) {
            if (entry.getValue().size() < queryK) {
                queue.add(entry.getKey());
                deleteSet.add(entry.getKey());
            }
        }
        while (queue.size() > 0) {//iteratively delete vertices whose degrees are less than k
            int curId = queue.poll();
            keepSet.remove(curId);
            Set<Integer> pnbSet = pnbMap.get(curId);
            for (int pnbId : pnbSet) {
                if (!deleteSet.contains(pnbId)) {
                    Set<Integer> tmpSet = pnbMap.get(pnbId);
                    tmpSet.remove(curId);
                    if (tmpSet.size() < queryK) {
                        addMoreNeighbors(pnbId, pnbId, 0, visitMap.get(pnbId), tmpSet, keepSet);
                        if (tmpSet.size() < queryK) {
                            queue.add(pnbId);
                            deleteSet.add(pnbId);
                        }
                    }
                }
            }
        }
        // obtain an undirected graph
        for (int id : keepSet) {
            Set<Integer> nbSet = pnbMap.get(id);
            for (int nbId : nbSet) {
                Set<Integer> tmpSet = pnbMap.get(nbId);
                if (tmpSet != null) tmpSet.add(id);
            }
        }
        int cnt = 0;
        while (!keepSet.isEmpty()) {
            int v = keepSet.iterator().next();
            BatchLinkerCSH ccFinder = new BatchLinkerCSH(graph, vertexType, edgeType, v, queryMPath, keepSet, pnbMap);
            Set<Integer> newSet = ccFinder.computeCC();
            System.out.println("new find size is :" + newSet.size());
            conSet.add(newSet);
            keepSet.removeAll(newSet);
            cnt += 1;
            System.out.println("进度条 is : " + cnt + "|" + keepSet.size());
        }

        //step 5: find the connected community
//		BatchLinkerCSH ccFinder = new BatchLinkerCSH(graph, vertexType, edgeType, queryMPath, keepSet, pnbMap);
//		return ccFinder.computeCC();
        return conSet;
    }

    private void findFirstKNeighbors(int startID, int curId, int index, List<Set<Integer>> visitList, Set<Integer> pnbSet) {
        int targetVType = queryMPath.vertex[index + 1], targetEType = queryMPath.edge[index];

        int nbArr[] = graph[curId];
        for (int i = 0; i < nbArr.length; i += 2) {
            int nbVertexID = nbArr[i], nbEdgeID = nbArr[i + 1];
            Set<Integer> visitSet = visitList.get(index + 1);
            if (targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID] && !visitSet.contains(nbVertexID)) {
                if (index + 1 < queryMPath.pathLen) {
                    findFirstKNeighbors(startID, nbVertexID, index + 1, visitList, pnbSet);
                    if (pnbSet.size() >= queryK) return;//we have found k meta-paths
                    visitSet.add(nbVertexID);
                } else {//a meta-path has been found
                    if (nbVertexID != startID) pnbSet.add(nbVertexID);
                    visitSet.add(nbVertexID);
                    if (pnbSet.size() >= queryK) return;//we have found k meta-paths
                }
            }
        }
    }

    private void addMoreNeighbors(int startID, int curId, int index, List<Set<Integer>> visitList, Set<Integer> pnbSet, Set<Integer> keepSet) {
        int targetVType = queryMPath.vertex[index + 1], targetEType = queryMPath.edge[index];

        int nbArr[] = graph[curId];
        for (int i = 0; i < nbArr.length; i += 2) {
            int nbVertexID = nbArr[i], nbEdgeID = nbArr[i + 1];
            Set<Integer> visitSet = visitList.get(index + 1);
            if (!visitSet.contains(nbVertexID) && targetVType == vertexType[nbVertexID] && targetEType == edgeType[nbEdgeID]) {
                if (index + 1 < queryMPath.pathLen) {
                    addMoreNeighbors(startID, nbVertexID, index + 1, visitList, pnbSet, keepSet);
                    if (pnbSet.size() >= queryK) return;//we have found k meta-paths
                    visitSet.add(nbVertexID);
                } else {//a meta-path has been found
                    if (keepSet.contains(nbVertexID)) {//restrict it to be in keepSet
                        if (nbVertexID != startID) {
                            pnbSet.add(nbVertexID);
                            visitSet.add(nbVertexID);
                            if (pnbSet.size() >= queryK) return;//we have found k meta-paths
                        }
                    }
                }
                visitSet.add(nbVertexID);//mark this vertex (and its branches) as visited
            }
        }
    }

    public static void main(String[] args) {
//		int graph[][] = new int[11][];
//		int a0[] = {};
//		graph[0] = a0;
//		int a1[] = { 7, 1, 8, 2 };
//		graph[1] = a1;
//		int a2[] = { 7, 3, 8, 4 };
//		graph[2] = a2;
//		int a3[] = { 8, 5, 9, 6 };
//		graph[3] = a3;
//		int a4[] = { 7, 7, 9, 8 };
//		graph[4] = a4;
//		int a5[] = { 8, 9, 9, 10 };
//		graph[5] = a5;
//		int a6[] = { 10, 11 };
//		graph[6] = a6;
//		int a7[] = { 1, 12, 2, 13, 4, 14 };
//		graph[7] = a7;
//		int a8[] = { 1, 15, 2, 16, 3, 17, 5, 18 };
//		graph[8] = a8;
//		int a9[] = { 3, 19, 4, 20, 5, 21 };
//		graph[9] = a9;
//		int a10[] = { 6, 22 };
//		graph[10] = a10;
//		int vertexType[] = { 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0 };
//		int edgeType[] = { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
//		int vertex1[] = { 1, 0, 1 }, edge1[] = { 3, 0 };
//		MetaPath metaPath1 = new MetaPath(vertex1, edge1);

        int graph[][] = new int[10][];
        int a0[] = {};
        graph[0] = a0;
        int a1[] = {5, 1, 6, 2};
        graph[1] = a1;
        int a2[] = {6, 3};
        graph[2] = a2;
        int a3[] = {7, 4};
        graph[3] = a3;
        int a4[] = {5, 5, 7, 6};
        graph[4] = a4;
        int a5[] = {1, 7, 4, 8, 8, 9};
        graph[5] = a5;
        int a6[] = {1, 10, 2, 11, 8, 12};
        graph[6] = a6;
        int a7[] = {3, 13, 4, 14, 9, 15};
        graph[7] = a7;
        int a8[] = {5, 16, 6, 17};
        graph[8] = a8;
        int a9[] = {7, 18};
        graph[9] = a9;
        int vertexType[] = {1, 1, 1, 1, 1, 0, 0, 0, 3, 3};
        int edgeType[] = {3, 3, 3, 3, 3, 3, 3, 0, 0, 2, 0, 0, 2, 0, 0, 2, 5, 5, 5};
        int vertex1[] = {1, 0, 3, 0, 1}, edge1[] = {3, 2, 5, 0};
        MetaPath metaPath1 = new MetaPath(vertex1, edge1);
        double weight[] = {};


        FastBCore BKCore = new FastBCore(graph, vertexType, edgeType);
        Set<Set<Integer>> conSet = BKCore.query(metaPath1, 2);
        Set<Integer> keepSet = new HashSet<Integer>();
        for (Set<Integer> subSet : conSet) {
            keepSet.addAll(subSet);
        }

        HomoGraphBuilder GP = new HomoGraphBuilder(graph, vertexType, edgeType, metaPath1, weight);
        for (Set<Integer> type1 : conSet) {
            BatchSearch association = GP.build(type1);
            Map<Integer, Set<Integer>> pnbMap = association.pnbMap;
//			Map<String, Integer> edgeAsso = association.edgeAsso;
//			Map<Integer, Set<String>> verAsso = association.verAsso;
            Map<String, Integer> numOfST = association.numOfST;

            pnbMap.forEach((key, value) -> {
                System.out.println(key + "    " + value);
            });
//			edgeAsso.forEach((key, value) -> {System.out.println(key + "    " + value);});
//			verAsso.forEach((key, value) -> {System.out.println(key + "    " + value);});
            numOfST.forEach((key, value) -> {
                System.out.println(key + "    " + value);
            });

        }
    }
}