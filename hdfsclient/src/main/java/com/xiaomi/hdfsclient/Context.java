package com.xiaomi.hdfsclient;

import java.util.HashMap;

class Context {
    private HashMap<Object, Object> countMapper;
    Object getCount(Object key) {
        return countMapper.get(key);
    }

    void setCount(Object key, Object value) {
        countMapper.put(key, value);
    }

    HashMap<Object, Object> getCountMapper() {
        return countMapper;
    }
}
