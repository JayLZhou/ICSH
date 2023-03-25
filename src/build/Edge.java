package build;

import java.io.IOException;
import java.util.Arrays;

public class Edge {
    private int[] key = new int[2];

    public Edge(int[] key) {
        if (key[0] > key[1]) {
            this.key[0] = key[1];
            this.key[1] = key[0];
        } else {
            this.key = key;
        }
    }

    public Edge(int start, int end) {
        if (start < end) {
            this.key[0] = start;
            this.key[1] = end;
        } else {
            this.key[0] = end;
            this.key[1] = start;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;//地址相等
        }

        if (obj == null) {
            return false;//非空性：对于任意非空引用x，x.equals(null)应该返回false。
        }

        if (obj instanceof Edge) {
            Edge other = (Edge) obj;
            //需要比较的字段相等，则这两个对象相等
            return other.key[0] == this.key[0] && this.key[1] == other.key[1];
        }
        return false;
    }

    @Override
    public int hashCode() {
        String keyStr = String.valueOf(key[0]) + String.valueOf(key[1]);
        return keyStr.hashCode();
    }
    public int[] getKey() {
        return this.key;
    }
}
