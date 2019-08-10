package com.xiaomi.hdfsclient;

public class CaseIgnoreWordCountMapper implements Mapper {

    public void map(String line, Context context) {
        String[] words = line.toUpperCase().split(" ");
        for (String word : words) {
            Integer value = (Integer) context.getCount(word);
            if (value == null) {
                context.setCount(word, 1);
            } else {
                context.setCount(word, value + 1);
            }
        }
    }
}
