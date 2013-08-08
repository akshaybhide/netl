package com.adnetik.bm_etl;

import java.util.*;
import com.adnetik.shared.*;
import java.io.*;


public class Hy_FirstTry
{

        public static void main(String[] args)
        {
                Util.pf("please type something....");
            try{
                String day=TimeUtil.getTodayCode();
                FileWriter fstream = new FileWriter("testfile"+day+".txt");
                BufferedWriter out = new BufferedWriter(fstream);
                Scanner sc=new Scanner(System.in);
                String line=sc.nextLine();
                int i=0;
                while (line!=null)
                {
                  if (line.equals("exit"))
                 {
                    out.close();
                    break;
                 }
                  out.write(line);
                  line=sc.nextLine();
                  i++;
                }

                Util.pf("write into file %d lines",i);
                out.close();
            }
           catch (Exception e){System.err.println("Error:"+e.getMessage());}
        }
}


