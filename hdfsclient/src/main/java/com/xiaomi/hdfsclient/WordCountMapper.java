package com.xiaomi.hdfsclient;

public class WordCountMapper implements Mapper {
    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Object value = context.get(word);
            if (null == value) {
                context.write(word, 1);
            } else {
                int intVal = (Integer) value;
                context.write(word, intVal + 1);
            }
        }
    }
}
