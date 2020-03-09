package com.conan.bigdata.common.javaapi;

import java.io.*;



public class TestSerializable {

    /**
     * 刚开始只定义name age字段， 进行序列化， 如果不加上这个serialVersionUID， 在修改字段属性后， 反序列化会失败, 报版本不对应的错
     * 加上这个serialVersionUID后， 序列化和反序列化，知道是同一个对象， 反序列化不会失败， 新增的字段会赋值默认值
     *
     * 序列化会把bean类的全路径，字段属性和值全部序列化起来， 用于网络传输和磁盘存储
     */
    private static class Customer implements Serializable {

        private static final long serialVersionUID = -5182532647273106745L;
        private String name;
        private int age;

        private double hight;

        public Customer(String name, int age) {
            this.name = name;
            this.age = age;
        }

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

        public double getHight() {
            return hight;
        }

        public void setHight(double hight) {
            this.hight = hight;
        }

        @Override
        public String toString() {
            return "name=" + name + ", age=" + age + ", hight=" + hight;
        }
    }


    private static void serializeObject() throws IOException {
        Customer customer = new Customer("hahaha", 25);
        // 对象输出流， 输出序列化的对象
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("D:\\opt\\serial.txt"));
        oos.writeObject(customer);
        System.out.println("Customer 对象序列化成功");
        oos.close();
    }

    private static Customer deserializeObject() throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("D:\\opt\\serial.txt"));
        Customer customer = (Customer) ois.readObject();
        System.out.println("Customer 对象反序列化成功");
        return customer;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        serializeObject();
//        Customer customer = deserializeObject();
//        System.out.println(customer);
    }
}