package com.ultimate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;

public class TooMuchIpFile {

    /**
     * 生成大文件
     *
     * @param ipFile
     * @param numberOfLine
     */
    public void gernBigFile(File ipFile, long numberOfLine) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        long startTime = System.currentTimeMillis();
        try {
            fw = new FileWriter(ipFile, true);
            bw = new BufferedWriter(fw);

            SecureRandom random = new SecureRandom();
            for (int i = 0; i < numberOfLine; i++) {
                bw.write("10." + random.nextInt(255) + "." + random.nextInt(255) + "." + random.nextInt(255) + "\n");
                if ((i + 1) % 1000 == 0) {
                    bw.flush();
                }
            }
            bw.flush();

            long endTime = System.currentTimeMillis();
            System.err.println(endTime - startTime);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fw != null) {
                    fw.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        // 1、第一次生成1亿(实际上最多为16581375)的ip地址，需要时间为3分多钟不到4分钟。

        TooMuchIpFile tooMuchIpFile = new TooMuchIpFile();
        File ipFile = new File("ipAddr.txt");
        try {
            ipFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
            tooMuchIpFile.gernBigFile(ipFile, 100000000);
        }
}
