package com.gridu.exsort;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import static com.gridu.exsort.FilesHandler.sortCollectionInsensitive;
import static com.gridu.exsort.LoggerHandler.logger;

public class ExternalSort {
    private static final int MAX_PART_SIZE = 1; // Max part size in Mbs
    private static String inputPathFile;

    public static void main(String[] args) throws IOException {
        logger.log(Level.INFO, "--- Start application ---");

        enterPathToFile();

        FilesHandler filesHandler = new FilesHandler(inputPathFile, MAX_PART_SIZE);

        RandomAccessFile raf = null;

        try {
            raf = filesHandler.openFileForReading();
        } catch (IOException e) {
            logger.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
        }

        for (int i = 0; i < filesHandler.getPartsCount(); i++) {
            List<String> strings = filesHandler.readPartAsStrings(raf, i);
            sortCollectionInsensitive(strings);
            filesHandler.savePartStringsInFile(strings, i);
        }

        filesHandler.closeFileStream(raf);
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
