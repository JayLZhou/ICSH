package advanced;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import build.Edge;
import util.MetaPath;
import util.deepCopy;
import build.BatchSearch;

public class TypeBinary {
    private int vertexType[] = null;//vertex -> type
    private double weight[] = null;
    private int queryK = -1;
    private MetaPath queryMPath = null;
    private double constraints[] = null;
    Map<Integer, Set<Integer>> pnbMap, tmpMap, deepMap = new HashMap<Integer, Set<Integer>>();
    Map<Edge, Integer> edgeAsso, tmpAsso, deepAsso = new HashMap<Edge, Integer>();
    Map<Integer, Set<Edge>> verAsso = new HashMap<Integer, Set<Edge>>();
    List<Integer> sortedType1, tmptype1, sortedType2, tmptype2, deeptype1, deeptype2 = new ArrayList<Integer>();

    public TypeBinary(int vertexType[], double weight[], int queryK, MetaPath queryMPath, Map<Integer, Set<Integer>> pnbMap,
                      BatchSearch association, List<Integer> sortedType1, List<Integer> sortedType2) {
        this.vertexType = vertexType;
        this.weight = weight;
        this.queryK = queryK;
        this.queryMPath = queryMPath;
        this.pnbMap = pnbMap;
        this.edgeAsso = association.edgeAsso;
        this.verAsso = association.verAsso;
        this.sortedType1 = sortedType1;
        this.sortedType2 = sortedType2;
    }

    public double getTypeMax(double csts[], int type, Set<Integer> community) {
        constraints = csts;
        tmptype1 = new ArrayList<Integer>(sortedType1);
        tmptype2 = new ArrayList<Integer>(sortedType2);
        deepCopy deepCopy = new deepCopy();
        tmpMap = deepCopy.copyMap(pnbMap);
        tmpAsso = deepCopy.copyAsso(edgeAsso);

        int minVer = 0;
        double f = -1;
        //delete vertices whose weight is smaller (or equal to) the constraints
        int i = 0;
        int checkV = sortedType1.get(i);
        while (i < sortedType1.size()) {
            checkV = sortedType1.get(i);
            if (weight[checkV] <= constraints[0] && tmptype1.contains(checkV)) {
                DeleVer(checkV, tmptype1);
            }
            i++;
        }
        i = 0;

//        int r = sortedType2.size() - 1;
        while (i < sortedType2.size()) {
            checkV = sortedType2.get(i);
            if (weight[checkV] < constraints[1] && tmptype2.contains(checkV)) {
                DeleVer(checkV, tmptype2);
            }
            i++;
        }
        if (tmptype1.isEmpty()) {
            return -1;
        }
        deeptype1 = new ArrayList<Integer>(tmptype1);
        deeptype2 = new ArrayList<Integer>(tmptype2);
        deepAsso = deepCopy.copyAsso(tmpAsso);
        deepMap = deepCopy.copyMap(tmpMap);
        // delete the second type vertices by binary searching.

        while (!tmptype1.isEmpty()) {
//            System.out.println("tmptype1 size is : " + tmptype1.size());

            if (type == queryMPath.getEndType()) {
                int l = 0, r = tmptype1.size() - 1;
                int cnt;
                while (l < r) {
                    int mid = (l + r) / 2;

                    cnt = 0;
                    while (!tmptype1.isEmpty() && cnt <= mid) {
                        community.clear();
                        community.addAll(tmptype1);
                        minVer = tmptype1.get(0);
                        DeleVer(minVer, tmptype1);
                        cnt++;
                    }
                    if (tmptype1.isEmpty()) {
                        r = mid;
                    } else {
                        l = mid + 1;
                    }
                    if (l != r) {
                        tmptype1 = deepCopy.copy(deeptype1);
                        tmpMap = deepCopy.copyMap(deepMap);
                        tmpAsso = deepCopy.copyAsso(deepAsso);
                    }
                }
                f = Double.MAX_VALUE;
                for (int vertex : tmptype1) {
                    f = Math.min(weight[vertex], f);
                }
                if (tmptype1.isEmpty()) {
                    f = weight[minVer];
                } else {
                    community.clear();
                    community.addAll(tmptype1);
                }

                return f;
            }
            if (type == queryMPath.getSecondType()) {
                if (tmptype2.isEmpty()) {
                    return f;
                }
                int l = 0, r = tmptype2.size() - 1;
                int cnt;
                while (l < r) {
                    int mid = (l + r) / 2;
                    cnt = 0;
                    while (!tmptype2.isEmpty() && cnt <= mid) {
                        minVer = tmptype2.get(0);
                        DeleVer(minVer, tmptype2);
                        cnt++;
                    }
                    // mid is larger
                    if (tmptype1.isEmpty()) {
                        r = mid;
                    } else {
                        l = mid + 1;
                    }
                    if (l != r) {
                        tmptype1 = deepCopy.copy(deeptype1);
                        tmptype2 = deepCopy.copy(deeptype2);
                        tmpMap = deepCopy.copyMap(deepMap);
                        tmpAsso = deepCopy.copyAsso(deepAsso);
                    }
                }
                cnt = 0;
//             find the theorehold value, and delete
                if (tmptype1.isEmpty()) {

                } else {
                    DeleVer(tmptype2.get(0), tmptype2);
                    if (tmptype1.isEmpty()) {
                        minVer = deeptype2.get(l);
                    }
                }
            }
            f = Math.max(f, weight[minVer]);
//            System.out.println("minVer = " + minVer);
        }

//		vertices.add(minVer);
        tmpMap.forEach((key, value) -> {
            if (value.size() >= queryK) System.out.println(key + "    " + value);
        });
        return f;
    }

    public void DeleVer(int v, List<Integer> vertices) {
        if (vertices.isEmpty()) {
            return;
        }
        if (vertices.contains(v)) {
            vertices.remove(vertices.indexOf(v));
        } else {
            return;
        }
        if (vertexType[v] == queryMPath.getEndType()) {
            Set<Integer> pnbSet = tmpMap.get(v);
            for (int pnb : pnbSet) {
                if (tmptype1.contains(pnb)) {
                    Set<Integer> tmpSet = tmpMap.get(pnb);
                    tmpSet.remove(v);
                    if (tmpSet.size() < queryK) {
                        DeleVer(pnb, tmptype1);
                    } else {
                        tmpMap.replace(pnb, tmpSet);
                    }
                }
            }
        } else {
            if (vertexType[v] == queryMPath.getSecondType()) {
                Set<Edge> endptsSet = verAsso.get(v);
                for (Edge endpts : endptsSet) {
                    int start = endpts.getKey()[0];
                    int end = endpts.getKey()[1];
                    int count = tmpAsso.get(endpts);
                    count = count - 1;
                    if (count == 0) {
                        Set<Integer> nb = tmpMap.get(start);
                        nb.remove(end);
                        if (nb.size() < queryK && tmptype1.contains(start)) {
                            DeleVer(start, tmptype1);
                        } else {
                            if (nb.size() >= queryK) {
                                tmpMap.replace(start, nb);
                            }
                        }
                        nb = tmpMap.get(end);
                        nb.remove(start);
                        if (nb.size() < queryK && tmptype1.contains(end)) {
                            DeleVer(end, tmptype1);
                        } else {
                            if (nb.size() >= queryK) {
                                tmpMap.replace(end, nb);
                            }
                        }
                    }
                    tmpAsso.replace(endpts, count);
                }
            }
        }
    }
}