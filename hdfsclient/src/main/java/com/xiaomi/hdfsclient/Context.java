package com.xiaomi.hdfsclient;

import java.util.HashMap;

class Context {
    private HashMap<Object, Object> countMapper = new HashMap<>();
    public Object getCount(Object key) {
        return countMapper.get(key);
    }

    public void setCount(Object key, Object value) {
        countMapper.put(key, value);
    }

    HashMap<Object, Object> getCountMapper() {
        return countMapper;
    }
}
