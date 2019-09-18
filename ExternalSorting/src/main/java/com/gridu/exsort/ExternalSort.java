package com.gridu.exsort;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;

import static com.gridu.exsort.LoggerHandler.logger;

public class ExternalSort {
    private static String inputPathFile;
    private static final int MAX_PART_SIZE = 10; // Max part size in Mbs

    public static void main(String[] args) {
        logger.log(Level.INFO, "--- Start application ---");

        enterPathToFile();

        FilesHandler filesHandler = new FilesHandler();
        filesHandler.createFileWithSizeSpecified("test1", 100);

    }

    public static void enterPathToFile() {
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
