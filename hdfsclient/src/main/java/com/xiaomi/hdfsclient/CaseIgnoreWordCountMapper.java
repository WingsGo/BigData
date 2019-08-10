package com.xiaomi.hdfsclient;

public class CaseIgnoreWordCountMapper implements Mapper {

    public void map(String line, Context context) {
        String[] words = line.toUpperCase().split(" ");
        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                context.write(word, (Integer)value + 1);
            }
        }
    }
}
