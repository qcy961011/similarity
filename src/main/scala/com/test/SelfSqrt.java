package com.test;

import java.math.BigDecimal;

public class SelfSqrt {
    public static void main(String[] args) {
        double num = 6.25;
        System.out.println(sqrtMathod(num));
    }

    public static double sqrtMathod(double num) {
        double sqrtNum = -1;
        boolean isFindSqrt = false;
        double tempSqrt = 0;
        if (num > 0) {
            if (num == 1) {
                return 1;
            } else {
                for (int j = 0; j <= num / 2 + 1; j++) {
                    if (j * j == num) {
                        sqrtNum = j;
                        isFindSqrt = true;
                        break;
                    }
                    if (j * j > num) {
                        tempSqrt = j - 1;
                        break;
                    }
                }
            }
        }
        if (!isFindSqrt) {
            sqrtNum = recuFindSqrt(num, tempSqrt, isFindSqrt);
        }
        return sqrtNum;
    }


    private static double recuFindSqrt(double num, double sqrtValue, boolean isFindSqrt) {
        double tempSqrt = 0;
        for (int i = 0; i < 10; i++) {
            tempSqrt = sqrtValue + i / 10.0;
            if (tempSqrt * tempSqrt == num) {
                isFindSqrt = true;
                sqrtValue = tempSqrt;
                break;
            } else if (tempSqrt * tempSqrt > num) {
                tempSqrt = add(sqrtValue, (i - 1) / 10.0);
                sqrtValue = tempSqrt;
                break;
            }
        }
        Double temp = new Double(tempSqrt);
        if (temp.toString().length() <= 16 && !isFindSqrt) {
            sqrtValue = recuFindSqrt(num, tempSqrt, isFindSqrt);
        }
        return sqrtValue;
    }

    public static double add(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.add(b2).doubleValue();
    }

}