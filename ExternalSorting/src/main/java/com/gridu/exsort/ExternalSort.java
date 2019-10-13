package com.gridu.exsort;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;

import static com.gridu.exsort.LoggerHandler.logger;

public class ExternalSort {
    private static final int MAX_PART_SIZE = 1; // Max part size in Mbs
    private static String inputPathFile;

    public static void main(String[] args) {
        logger.log(Level.INFO, "--- Start application ---");

        enterPathToFile();

        FilesHandler filesHandler = new FilesHandler(inputPathFile, MAX_PART_SIZE);

        filesHandler.divideIntoSortedParts();
        filesHandler.mergingIntoOne();

        logger.log(Level.INFO, "--- Stop application ---");
    }

    private static void enterPathToFile() {
        System.out.println("Enter path to file for sorting");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            try {
                inputPathFile = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                logger.log(Level.SEVERE, e.getMessage());
            }

            if (!inputPathFile.trim().isEmpty()) {
                logger.log(Level.INFO, "Your path: " + inputPathFile);
                break;
            } else {
                System.out.println("Try again");
                logger.log(Level.SEVERE, "WTF MAN?");
            }
        }
    }
}
