package com.youmeng.taoshelf;

import com.youmeng.taoshelf.util.UUIDUtils;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;

public class PublicTest {

    @Test
    public void test() {
        Date start = new Date();
        Date end = new Date(start.getTime() + 1800000L);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
        System.out.println(format.format(start));
        System.out.println(format.format(end));
    }

    @Test
    public void test1() {
        int total = 15000;
        String s1;
        String s2;
        s1 = "1-" + total / 400;
        s2 = total / 400 + 1 + "-" + (total / 200 + 1);
        System.out.println(s1);
        System.out.println(s2);
    }

    int i = 0;

    @Test
    public void test2() {
        t();
        System.out.println(i);
    }

    public void t() {
        i = i + 2;
    }

    @Test
    public void test3() {
        for (i = 0; i < 10; i++) {
            System.out.println(UUIDUtils.getSerialNum());
        }
    }

    @Test
    public void tes4() {
        ArrayDeque<Integer> deque = new ArrayDeque<>();
        for (int i = 0; i < 10; i++) {
            deque.push(i);
        }
        while (!deque.isEmpty()) {
            Integer pop = deque.pop();
            if (pop == 5) {
                deque.add(58);
            } else {
                System.out.println(pop);
            }
        }
    }
}
