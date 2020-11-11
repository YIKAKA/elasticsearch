package com.sxy.es.estest0701.util;

import java.io.*;

public class ReadGeoNames {

  public static void main(String[] args) throws IOException {
    File file = new File("F:\\allCountries\\allCountries_01\\allCountries_01_01\\allCountries_01_01_01.txt");
    BufferedReader reader = null;
    String temp = null;
    int line = 1;
    reader = new BufferedReader(new FileReader(file));
    while((temp=reader.readLine())!=null){
      System.out.println(temp);
      String []arrayStr = temp.split("\\t+");
      for (String s : arrayStr) {
        System.out.println(s);
      }
      line ++;
    }
  }
}
