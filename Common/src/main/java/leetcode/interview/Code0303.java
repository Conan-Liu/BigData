package leetcode.interview;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
/**
 * 堆盘子。设想有一堆盘子，堆太高可能会倒下来。因此，在现实生活中，盘子堆到一定高度时，我们就会另外堆一堆盘子。
 * 请实现数据结构SetOfStacks，模拟这种行为。SetOfStacks应该由多个栈组成，并且在前一个栈填满时新建一个栈。
 * 此外，SetOfStacks.push()和SetOfStacks.pop()应该与普通栈的操作方法相同（也就是说，pop()返回的值，应该跟只有一个栈时的情况一样）。
 * 进阶：实现一个popAt(int index)方法，根据指定的子栈，执行pop操作。
 * 当某个栈为空时，应当删除该栈。当栈中没有元素或不存在该栈时，pop，popAt 应返回 -1.
 */
public class Code0303 {

    /**
     * 栈结构可以自己实现，也可以使用{@link Stack}
     * 各个栈之间使用顺序可以使用List或数组
     */
    private int capacity;
    private List<Stack<Integer>> list;
    public Code0303(int cap){
        // 栈的容量
        this.capacity=cap;
        this.list=new ArrayList<>();
    }

    public void push(int val){
        if(capacity<=0){
            return;
        }
        if(list.isEmpty()){
            list.add(new Stack<>());
        }
        if(list.get(list.size()-1).size()>= capacity){
            list.add(new Stack<>());
        }
        Stack<Integer> stack=list.get(list.size()-1);
        stack.push(val);
    }

    public int pop(){
        if(list.size()<=0){
            return -1;
        }
        Stack<Integer> stack=list.get(list.size()-1);
        Integer p=stack.pop();
        if(stack.isEmpty()){
            list.remove(list.size()-1);
        }
        return p;
    }

    public int popAt(int index){
        if(list.size()<=0||index>list.size()-1){
            return -1;
        }
        Stack<Integer> stack=list.get(index);
        Integer pop = stack.pop();
        if(stack.isEmpty()){
            list.remove(index);
        }
        return pop;
    }
}
