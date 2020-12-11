package org.manymango.example.function;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lcx
 * @date 2020/12/11
 **/
public class Constant {

    public static Map<String, String> map = new HashMap<>();


    public static void main(String[] args) {
        TestClass testClass = new TestClass();

        new Thread(() -> {
            int index = 0;
            while (index<100000) {
                index++;
                testClass.iPlugPlus(1);
            }
            testClass.iPlugPlus(2);
        }).start();

        new Thread(() -> {
            int index = 0;
            while (index<100000) {
                index++;
                testClass.iPlugPlus(1);
            }
            testClass.iPlugPlus(2);
        }).start();

        new Thread(() -> {
            int index = 0;
            while (index<100000) {
                index++;
                testClass.iPlugPlus(1);
            }
            testClass.iPlugPlus(2);
        }).start();



    }


    public static class TestClass{
        private int i;


        public TestClass() {
        }

        public void iPlugPlus(int type){
            if (type == 1) {
                i++;
            } else {
                System.out.println(i);
            }
        }

    }


}
