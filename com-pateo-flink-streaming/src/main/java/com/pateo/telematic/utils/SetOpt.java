package com.pateo.telematic.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SetOpt {
	  
    public List<Integer> intersect(List<Integer> ls, List<Integer> ls2) {   
		List<Integer> list = new ArrayList<Integer>();   
//		List<Integer> list = new ArrayList<Integer>(Arrays.asList(new Integer[ls.size()]));   

        Collections.copy(list, ls);   
        list.retainAll(ls2);   
        return list;   
    }   
  
	public List<Integer> union(List<Integer> ls, List<Integer> ls2) {   
		List<Integer> list = new ArrayList<Integer>(Arrays.asList(new Integer[ls.size()]));   
        Collections.copy(list, ls);   
       list.addAll(ls2);   
        return list;   
    }   
  
    public List<Integer> diff(List<Integer> ls, List<Integer> ls2) {   
        List<Integer> list = new ArrayList<Integer>(Arrays.asList(new Integer[ls.size()]));   
        Collections.copy(list, ls);   
        list.removeAll(ls2);   
        return list;   
    }   
    public static void main(String[] args) {   
        SetOpt opt = new SetOpt();   
        List<Integer> l1 = new ArrayList<Integer>();   
        l1.add(1);   
        l1.add(2);   
        l1.add(3);   
        l1.add(4);   
        List<Integer> l2 = new ArrayList<Integer>();   
        l2.add(3);   
        l2.add(4);   
        l2.add(5);   
        l2.add(6);   
        List<?> intersectList = opt.intersect(l1, l2);   
        System.out.println("交集：");   
        for (int i = 0; i < intersectList.size(); i++) {   
            System.out.print(intersectList.get(i) + " ");   
        }   
        System.out.println();   
        List<?> unionList = opt.union(l1, l2);   
        System.out.println("并集：");   
        for (int i = 0; i < unionList.size(); i++) {   
            System.out.print(unionList.get(i) + " ");   
        }   
        System.out.println();   
        List<?> diffList = opt.diff(l1, l2);   
        System.out.println("差集：");   
        for (int i = 0; i < diffList.size(); i++) {   
            System.out.print(diffList.get(i) + " ");   
        }   
        System.out.println();   
    }   
  
}  
