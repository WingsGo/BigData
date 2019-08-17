package com.xiaomi.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order> {
    private String order_id;
    private String user_id;
    private String pdtName;
    private float price;
    private int number;
    private double amountFee;

    public void set(String order_id, String user_id, String pdtName, float price, int number, double amountFee) {
        this.order_id = order_id;
        this.user_id = user_id;
        this.pdtName = pdtName;
        this.price = price;
        this.number = number;
        this.amountFee = amountFee;
    }

    public double getAmountFee() {
        return amountFee;
    }

    public void setAmountFee(float amountFee) {
        this.amountFee = amountFee;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getPdtName() {
        return pdtName;
    }

    public void setPdtName(String pdtName) {
        this.pdtName = pdtName;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    @Override
    public String toString() {
        return this.order_id + ", " + this.user_id + ", " + this.pdtName + ", " + this.price + ", " + this.number
                + ", " + this.amountFee;
    }

    @Override
    public int compareTo(Order o) {
        int result = Double.compare(o.getAmountFee(), this.getAmountFee());
        return result == 0 ? this.pdtName.compareTo(o.getPdtName()) : result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.order_id);
        dataOutput.writeUTF(this.user_id);
        dataOutput.writeUTF(this.pdtName);
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.number);
        dataOutput.writeDouble(this.amountFee);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readUTF();
        this.user_id = dataInput.readUTF();
        this.pdtName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.number = dataInput.readInt();
        this.amountFee = dataInput.readDouble();
    }
}
