package com.conan.bigdata.common.algorithm;

import java.util.PriorityQueue;

import com.conan.bigdata.common.javaapi.PriorityQueueExp;

/**
 * 针对TopN的问题，比较好的方法是  利用hash把大文件分解成若干小文件， 使用构造的小根堆来计算最大的N个数
 * 1. hash分成小文件
 * 2. 小根堆，可以通过{@link PriorityQueue}来实现小根堆，参考{@link PriorityQueueExp}
 */
public class TopN {


}
