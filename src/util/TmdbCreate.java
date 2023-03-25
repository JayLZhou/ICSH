package util;

import java.io.*;
import java.util.*;

/**
 * @author fangyixiang
 * @date Sep 17, 2018 read all the data
 */
public class TmdbCreate {
    public String rootPath = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/tmdb";
    public String dst_root = "/Users/zhouxiaolun/MasterLibrary/graphaTherory/InfluentialCommunities/dataset/tmdb";
    public Map<Integer, Integer> id2type = new HashMap<>();
    public Map<Integer, Integer> type2cnt = new HashMap<>();
    public ArrayList<Map<Integer, Set<Integer>>> allMap = new ArrayList<>();
    public Map<Integer, Set<Integer>> pnbMap = new HashMap<>();


    int[][] graph = null; // for create graph.txt;
    String[] nodeVertex = new String[]{"movie", "cast", "company", "country", "director", "genre", "keyword"};

    private void readAllNode() {
        try {
            int cnt = 0;
            BufferedWriter bufferedWriterVertex = new BufferedWriter(new FileWriter(dst_root + "/" + "vertex.txt"));
            BufferedWriter bufferedWriterWeight = new BufferedWriter(new FileWriter(dst_root + "/" + "weight.txt"));

            for (int i = 0; i < nodeVertex.length; i++) {
                String nodePath = rootPath + "/" + "node_importance-" + nodeVertex[i] + ".txt";
                BufferedReader stdin = new BufferedReader(new FileReader(nodePath));
                String line = null;
                while ((line = stdin.readLine()) != null) {
                    String s[] = line.split("\\s+");
                    if (type2cnt.containsKey(i)) {
                        Integer count = type2cnt.get(i);
                        type2cnt.replace(i, count + 1);
                    } else {
                        type2cnt.put(i, 1);
                    }
                    System.out.println(cnt + "|" + i);
                    bufferedWriterVertex.write(Integer.toString(cnt) + " " + Integer.toString(i) + "\n");
                    bufferedWriterWeight.write(Integer.toString(cnt) + " " + s[1] + "\n");
                    id2type.put(cnt, i);
                    cnt += 1;
                }
            }
            bufferedWriterVertex.close();
            bufferedWriterWeight.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getAllMap() {
    }

    public static void main(String[] args) throws IOException {

        TmdbCreate graphCreater = new TmdbCreate();
        graphCreater.readAllNode();
        graphCreater.createGraph();

    }

    private void createGraph() throws IOException {
        int cnt = 0;
        for (int i = 0; i < nodeVertex.length; i++) {
            int vertexId = i;
            String left = nodeVertex[i];

            for (int j = 0; j < nodeVertex.length; j++) {
                if (i == j) {
                    continue;
                }
                String right = nodeVertex[j];
                String edgePath = rootPath + "/" + left + "-" + right + ".txt";
                FileReader reader;
                try {
                    reader = new FileReader(edgePath);
                } catch (FileNotFoundException e) {
                    continue;
                }
                BufferedReader stdin = new BufferedReader(reader);
                String line = null;
                while ((line = stdin.readLine()) != null) {
                    String s[] = line.split("\\s+");
                    int leftId = getVertexId(Integer.parseInt(s[0]), i);
                    int rightId = getVertexId(Integer.parseInt(s[1]), j);
                    if (pnbMap.containsKey(leftId)) {
                        pnbMap.get(leftId).add(rightId);
                    } else {
                        Set<Integer> tmpSet = new HashSet<>();
                        tmpSet.add(rightId);
                        pnbMap.put(leftId, tmpSet);
                    }

                    //
                    if (pnbMap.containsKey(rightId)) {
                        pnbMap.get(rightId).add(leftId);
                    } else {
                        Set<Integer> tmpSet = new HashSet<>();
                        tmpSet.add(leftId);
                        pnbMap.put(rightId, tmpSet);
                    }
                }
            }
        }
        Set<Integer> addSet = new HashSet<>();
        System.out.println("build map over ");
        int maxSize = 0;
        int maxKey = 0;
        for (int key : pnbMap.keySet()) {
            if (pnbMap.get(key).size() > maxSize) {
                maxSize = pnbMap.get(key).size();
                maxKey = key;
                System.out.println("new max size key is : " + key + "|" + "is size is : " + pnbMap.get(key).size() + "｜" + "type is :" + id2type.get(key));
            }
        }

        addSet.add(maxKey);
        addSet.add(59402);
        addSet.add(598);
//        addSet.add(62150);
//        addSet.add(62145);
//        addSet.add(62155);
//        addSet.add(218);
        maxSize = 0;
        for (int key : pnbMap.keySet()) {
            if (id2type.get(key) == 5 ) {
                maxSize = pnbMap.get(key).size();
                maxKey = key;
                System.out.println("new max size key is : " + key + "|" + "is size is : " + pnbMap.get(key).size() + "｜" + "type is :" + id2type.get(key));
            }
        }
        // get the sub-graph
        // 从id2type里删除百分之5
        Random random = new Random();
        int len = id2type.keySet().size();

        for (int i = 0; i < 0.15 * len ; i++) {
            int id = random.nextInt(id2type.keySet().size() - 1);
            while ( addSet.contains(id) ) {
                id = random.nextInt(id2type.keySet().size() - 1);
            }
            addSet.add(id);
            id2type.remove(id);
            if (!pnbMap.containsKey(id)) {
                continue;
            }
            for (int nb : pnbMap.get(id)) {
                pnbMap.get(nb).remove(id);
            }
            pnbMap.remove(id);
        }

//        for (; addSet.size() < 0.10 * type2cnt.get(5); ) {
//            int id = random.nextInt(type2cnt.keySet().size() - 1);
//            while (id2type.get(id) != 5) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//            }
//            if (id2type.get(id) == 5) {
//                addSet.add(id);
//            } else {
//                System.out.println("sdsdsdsdsd");
//
//            }
//        }
//        for (; addSet.size() < 0.25 * type2cnt.get(3); ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 3) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
//        for (; addSet.size() < 0.1 * type2cnt.get(0); ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 0) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
//        for (; addSet.size() < 0.3 * type2cnt.get(0); ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 0) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
//        for (; addSet.size() < 0.2 * type2cnt.get(3) + 0.2 * type2cnt.get(0); ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 0) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
//        for (; addSet.size() < 0.2 * type2cnt.get(3) + 0.2 * type2cnt.get(0) + 0.2 * type2cnt.get(1); ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 0) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
//        for (; addSet.size() < 0.2 * type2cnt.get(3) + 0.2 * type2cnt.get(0) + 0.2 * type2cnt.get(1) + type2cnt.get(4) * 0.2; ) {
//            int id = random.nextInt(id2type.keySet().size() - 1);
//            while (id2type.get(id) != 0) {
//                id = random.nextInt(id2type.keySet().size() - 1);
//                addSet.add(id);
//            }
//        }
        for (Integer id : addSet) {
            id2type.remove(id);
            if (!pnbMap.containsKey(id)) {
                continue;
            }
            for (int nb : pnbMap.get(id)) {
                pnbMap.get(nb).remove(id);
            }
            pnbMap.remove(id);
        }
        BufferedWriter bufferedWriterEdge = new BufferedWriter(new FileWriter(dst_root + "/" + "edge.txt"));
        BufferedWriter bufferedWriterGraph = new BufferedWriter(new FileWriter(dst_root + "/" + "graph.txt"));

        int cntEdge = 0;
        for (int key = 0; key < len; key++) {

            if (!pnbMap.containsKey(key)) {
                bufferedWriterGraph.write(Integer.toString(key) + "\n");
                continue;
            }
            bufferedWriterGraph.write(Integer.toString(key));

            for (int val : pnbMap.get(key)) {

                bufferedWriterGraph.write(" " + val + " " + Integer.toString(cntEdge));
                bufferedWriterEdge.write(Integer.toString(cntEdge) + " " + Integer.toString(getEdgeType(key, val)) + "\n");
                cntEdge += 1;
            }
            bufferedWriterGraph.write("\n");
        }
        bufferedWriterEdge.close();
        bufferedWriterGraph.close();
    }

    private int getVertexType(int id) {
        int t = 0;
        while (id >= 0) {
            id -= type2cnt.get(t);
            if (id <= 0) {
                break;
            }
            t += 1;
        }
        return t;
    }

    private int getEdgeType(int left, int right) {
        int leftType = id2type.get(left);
        int rightType = id2type.get(right);

        return getEdgeId(leftType, rightType);
    }


    private int getVertexId(int id, int i) {
        int offset = 0;
        for (int k = 0; k < i; k++) {
            offset += type2cnt.get(k);
        }
        return id + offset;

    }

    private int getEdgeId(int i, int j) {
        // String[] nodeVertex = new String[]{"movie", "cast", "company", "country", "director", "genre", "keyword"};
        // movie - cast : 0
        // cast - movie : 1
        // movie - company : 2
        // company - movie : 3
        // movie - country : 4
        // country - moive : 5
        // movie - director : 6
        // director - movie : 7
        // moive - genre : 8
        // genre - movie : 9
        // movie - keyword : 10
        // keyword - movie : 11
        if (i == 0) {
            return (j * 2) - 2;
        } else {
            if (j != 0) {
                System.out.println("不可能");
            }
            return (i * 2) - 1;
        }
    }

    private void analysize() {

    }


}