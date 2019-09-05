package com.test;

public class Birthday {
    public static void main(String[] args) {
        System.out.println(peopleNum(1 , 1.0));
    }


    /**
     * 当 1 - (365 / 365) * ( 364 / 365) * ( 363 / 365)... > 0.8
     * (365 / 365) * ( 364 / 365) * ( 363 / 365)... < 0.2
     * 则可以判断有80%的几率会重复
     * @param number
     * @param probability
     * @return
     */

    public static int peopleNum(int number , double probability){
        int num = 365 - number;
        if ((probability = probability * (num / 365.0)) > 0.2){
            number = peopleNum(++number , probability);
        }
        return number;
    }
}
