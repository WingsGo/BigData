package com.xiaomi.mapreduce;

import com.sun.tools.corba.se.idl.constExpr.Or;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class OrderTopN {
    public static class OrderTopNMapper extends Mapper<IntWritable, Text, Text, Order> {

        static Order order;
        static Text tmpKey;


        static {
            order = new Order();
            tmpKey = new Text();
        }

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            order.set(split[0], split[1], split[2], Float.parseFloat(split[3]),
                    Integer.parseInt(split[4]), Double.parseDouble(split[5]));
            tmpKey.set(split[0]);
            // 对象序列化后存储
            context.write(tmpKey, order);
        }
    }

    public static class OrderTopNReducer extends Reducer<Text, Order, Order, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Order> values, Context context) throws IOException, InterruptedException {
            // values迭代器返回同一对象，仅仅值不同
            ArrayList<Order> orders = new ArrayList<>();
            for (Order order : values) {
                Order order1 = new Order();
                order1.set(order.getOrder_id(), order.getUser_id(), order.getPdtName(), order.getPrice(), order.getNumber(), order.getAmountFee());
                orders.add(order1);
            }

            Collections.sort(orders);

            for (int i=0; i<3; ++i) {
                context.write(orders.get(i), NullWritable.get());
            }
        }
    }
}
