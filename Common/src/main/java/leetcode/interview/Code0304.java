package leetcode.interview;

import java.util.Stack;

/**
 * 实现一个MyQueue类，该类用两个栈来实现一个队列。
 * 示例：
 * MyQueue queue = new MyQueue();
 * queue.push(1);
 * queue.push(2);
 * queue.peek();  // 返回 1
 * queue.pop();   // 返回 1
 * queue.empty(); // 返回 false
 */
public class Code0304 {

    private Stack<Integer> stack;
    private Stack<Integer> reverseStack;

    public Code0304() {
        this.stack = new Stack<>();
        this.reverseStack = new Stack<>();
    }

    public void push(int x) {
        stack.push(x);
    }

    public int pop() {
        peek();
        return reverseStack.pop();
    }

    // 核心
    public int peek() {
        if (reverseStack.isEmpty()) {
            while (!stack.isEmpty()) {
                reverseStack.push(stack.pop());
            }
        }
        return reverseStack.peek();
    }

    // 两个栈都为空是，表示队列为空
    public boolean isEmpty(){
        return stack.isEmpty() && reverseStack.isEmpty();
    }
}
