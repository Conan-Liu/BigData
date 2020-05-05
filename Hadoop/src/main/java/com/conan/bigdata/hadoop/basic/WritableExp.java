package com.conan.bigdata.hadoop.basic;

import com.conan.bigdata.hadoop.mr.SelfDataTypeAndSecondSort;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 演示自定义数据类
 * TextInputFormat可以将<LongWritable,Text>的键值对输入map
 * 自定义数据类，可以让键值对<K,V>按照自己定义的数据类型输入map
 * 注意：自定义数据类型，一定要是可以序列化的，可以通过hadoop提供Writable接口来实现序列化方法
 * 只需要实现 write 和 readFields 方法即可
 *
 * MapReduce参考 {@link SelfDataTypeAndSecondSort}
 */
public class WritableExp implements Writable {

    private String name;
    private int age;
    private String sex;

    public WritableExp() {
    }

    public WritableExp(String name, int age, String sex) {
        set(name, age, sex);
    }

    public void set(String name, int age, String sex) {
        this.name = name;
        this.age = age;
        this.sex = sex;
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

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeUTF(this.sex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.age = in.readInt();
        this.sex = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WritableExp that = (WritableExp) o;

        if (age != that.age) return false;
        if (!name.equals(that.name)) return false;
        return sex.equals(that.sex);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + age;
        result = 31 * result + sex.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "V[" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ']';
    }
}
