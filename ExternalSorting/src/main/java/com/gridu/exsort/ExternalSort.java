package com.gridu.exsort;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
public class ExternalSort {
    public static final int MAX_PART_SIZE = 1; // Max part size in Mbs
    public static final int PART_CHUNK_STRINGS_FACTOR = 10; //10(%) - size personal buffer chunk of all strings part

    public static void main(String[] args) {
        log.info("--- Start application ---");
        ExternalSort externalSort = new ExternalSort();
        String inputPathFile = externalSort.enterPathToFile();

        FileHandler fileHandler = new FileHandler(inputPathFile, MAX_PART_SIZE);
        fileHandler.processInternalSorting();

        log.info("--- Stop application ---");
    }

    private String enterPathToFile() {
        System.out.println("Enter path to file for sorting");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String inputPath = null;

        while (true) {
            try {
                inputPath = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                log.error(e.getMessage());
            }
            if (StringUtils.isNotBlank(inputPath)) {
                log.debug("Your path: " + inputPath);
                break;
            } else {
                System.out.println("Try again");
                log.debug("Wrong Path?");
            }
        }
        try {
            reader.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return inputPath;
    }
}
