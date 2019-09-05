package com.test;

public class Bus {
    public static void main(String[] args) {
        System.out.println(Math.ceil(takeBusNum(1000)));
    }
    /**
     * 理论上的计算公式
     * 20 * 0.7
     * +（10 + 20） * 0.3 * 0.7
     * +（10 + 10 + 20） * 0.3 * 0.3 * 0.7
     * ...
     * @param iterTimes
     * @return
     */
    private static double takeBusNum(int iterTimes) {
        if (iterTimes == 1) {
            return 20 * 0.7;
        }
        return (10 * (iterTimes - 1) + 20) * Math.pow(0.3 , iterTimes - 1)
                + takeBusNum(iterTimes - 1);
    }
}
