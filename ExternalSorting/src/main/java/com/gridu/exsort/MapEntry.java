package com.gridu.exsort;

import java.util.Comparator;
import java.util.Map;

final class MapEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private V value;

    public MapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapEntry<?, ?> mapEntry = (MapEntry<?, ?>) o;

        if (!key.equals(mapEntry.key)) {
            return false;
        }
        return value.equals(mapEntry.value);
    }

    public static final Comparator<MapEntry<Integer, String>> compareByValueInsensitiveOrder = (o1, o2) -> {
        String s1 = o1.value;
        String s2 = o2.value;
        int n1 = s1.length();
        int n2 = s2.length();
        int min = Math.min(n1, n2);
        for (int i = 0; i < min; i++) {
            char c1 = s1.charAt(i);
            char c2 = s2.charAt(i);
            if (c1 != c2) {
                c1 = Character.toUpperCase(c1);
                c2 = Character.toUpperCase(c2);
                if (c1 != c2) {
                    c1 = Character.toLowerCase(c1);
                    c2 = Character.toLowerCase(c2);
                    if (c1 != c2) {
                        // No overflow because of numeric promotion
                        if (c1 - c2 == 0) {
                            return +1;
                        }
                        return c1 - c2;
                    }
                }
            }
        }
        if (n1 - n2 == 0) {
            return +1;
        }
        return n1 - n2;
    };
}
