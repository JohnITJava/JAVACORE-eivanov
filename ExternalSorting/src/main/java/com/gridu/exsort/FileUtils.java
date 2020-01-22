package com.gridu.exsort;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.Random;

@UtilityClass
public class FileUtils {

    public static void createFileWithRandomSymbols(int sizeinMb,
                                                   String fileName,
                                                   int maxStringLength,
                                                   boolean includeLetters,
                                                   boolean includeNumbers) throws IOException {
        File file = new File(fileName);
        FileOutputStream fos = new FileOutputStream(file);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        String randomString;
        int randomCount = 1;

        while (file.length() < sizeinMb * 1000000) {
            randomCount = new Random().nextInt(maxStringLength) + 1;
            randomString = RandomStringUtils.random(randomCount, includeLetters, includeNumbers);

            bw.write(randomString);
            bw.newLine();
        }
        String lastRandomString = RandomStringUtils.random(randomCount, includeLetters, includeNumbers);
        bw.write(lastRandomString);
        bw.flush();
        bw.close();
    }

    public static void createFileWithSizeSpecified(String filename, int sizeInMb) {
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(filename, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            f.setLength(sizeInMb * 1024 * 1024);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void eraseLastLineInFile(String filePath) {
        File file = new File(filePath);
        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            byte b;
            long length = f.length() - 1;
            do {
                length -= 1;
                f.seek(length);
                b = f.readByte();
            } while (b != 10 && length > 0);
            f.setLength(length + 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
