package leetcode.offer;

import java.util.Stack;

/**
 * 剑指 Offer 31. 栈的压入、弹出序列
 * 输入两个整数序列，第一个序列表示栈的压入顺序，请判断第二个序列是否为该栈的弹出顺序。假设压入栈的所有数字均不相等。例如，序列 {1,2,3,4,5} 是某栈的压栈序列，序列 {4,5,3,2,1} 是该压栈序列对应的一个弹出序列，但 {4,3,5,1,2} 就不可能是该压栈序列的弹出序列。
 * 示例 1：
 * 输入：pushed = [1,2,3,4,5], popped = [4,5,3,2,1]
 * 输出：true
 * 解释：我们可以按以下顺序执行：
 * push(1), push(2), push(3), push(4), pop() -> 4,
 * push(5), pop() -> 5, pop() -> 3, pop() -> 2, pop() -> 1
 * 示例 2：
 * 输入：pushed = [1,2,3,4,5], popped = [4,3,5,1,2]
 * 输出：false
 * 解释：1 不能在 2 之前弹出。
 */
public class Code31 {

    public static boolean validateStackSequences(int[] pushed, int[] popped) {
        int len1 = pushed.length;
        int len2 = popped.length;
        if (len1 == 0 || len1 != len2)
            return false;
        Stack<Integer> stack = new Stack<>();
        stack.push(pushed[0]);
        int i = 1, j = 0;
        while (i < len1 || j < len1) {
            if ((!stack.isEmpty()) && stack.peek() == popped[j]) {
                stack.pop();
                j++;
            } else if (i < len1) {
                stack.push(pushed[i]);
                i++;
            } else {
                return false;
            }
        }

        return true;
    }

    public static void main(String[] args) {
        int[] pushed = new int[]{1, 2, 3, 4, 5};
        int[] poped = new int[]{4,5,3,2,1};
        System.out.println(validateStackSequences(pushed, poped));
    }
}