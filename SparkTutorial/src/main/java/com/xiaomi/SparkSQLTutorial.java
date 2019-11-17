package com.xiaomi;

import org.apache.hadoop.fs.Hdfs;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSQLTutorial {
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession session = SparkSession
                .builder()
                .appName("Spark SQL")
                .getOrCreate();
        String peoplePath = "hdfs://10.232.33.211:9000/people.json";
        Dataset<Row> df = session.read().json(peoplePath);
        // use spark sql with api
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();

        // use spark sql with SQL
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = session.sql("SELECT * FROM people");
        sqlDF.show();

        // use spark sql with system preserved database global_temp
        df.createOrReplaceGlobalTempView("people");
        session.sql("SELECT * FROM global_temp.people").show();
        session.newSession().sql("SELECT * FROM global_temp.people").show();

        // DataSet
        Person person = new Person();
        person.setName("Andy");
        person.setAge(31);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = session.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = session.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
        transformDS.collect();

        // DF can be convert to a DataSet by providing a class. Mapping based on name
        Dataset<Person> peopleDS = session.read().json(peoplePath).as(personEncoder);
        peopleDS.show();

        // Interoperating with RDDs
        String textPath = "hdfs://10.232.33.211:9000/people.txt";
        JavaRDD<Person> personJavaRDD = session.read().textFile(textPath).javaRDD().map(line -> {
            String[] parts = line.split(",");
            Person tmpPerson = new Person();
            tmpPerson.setName(parts[0]);
            tmpPerson.setAge(Integer.parseInt(parts[1].trim()));
            return tmpPerson;
        });
        Dataset<Row> peopleDF = session.createDataFrame(personJavaRDD, Person.class);
        peopleDF.createOrReplaceGlobalTempView("people");
        Dataset<Row> teenagersDF = session.sql("SELECT * FROM people WHERE age BETWEEN 13 AND 19");
        Encoder<String> stringEncoders = Encoders.STRING();
        teenagersDF.show();
        Dataset<String> teenagerNameByIndexDF = teenagersDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(1), stringEncoders);
        teenagerNameByIndexDF.show();
        Dataset<String> teenagerNameByFieldDF = teenagersDF.map((MapFunction<Row, String>) row -> "Name: " + row.getAs("name"), stringEncoders);
        teenagerNameByFieldDF.show();

        JavaRDD<String> peopleRDD = session.sparkContext().textFile(textPath, 1).toJavaRDD();
        String schemaString = "name age";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowJavaRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> peopleDataFrame = session.createDataFrame(rowJavaRDD, schema);
        peopleDataFrame.createOrReplaceTempView("people");
        Dataset<Row> results = session.sql("SELECT name FROM people");
        Dataset<String> nameDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING()
        );
        nameDS.write().save("hdfs://10.232.33.211:9000/DataFrame.txt");
        session.close();
    }
}
