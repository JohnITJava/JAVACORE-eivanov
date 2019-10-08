package com.gridu.exsort;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import static com.gridu.exsort.LoggerHandler.logger;

@Data
@NoArgsConstructor
public class FilesHandler {
    private Path pathToInputFile;
    private byte[] buffer;
    private int partsCount;
    private int partSizeMb;
    private List<String> strings = new ArrayList<>();
    private Long fileSize;
    private Long filePointer;

    public FilesHandler(String inputFilePath, int partSizeMb) {
        this.buffer = new byte[partSizeMb * 1024 * 1024];
        this.pathToInputFile = Paths.get(inputFilePath);
        this.fileSize = pathToInputFile.toFile().length();
        this.partSizeMb = partSizeMb;
        this.partsCount = (int) Math.ceil((double) fileSize / (partSizeMb * 1024 * 1024));
    }

    public static void sortCollectionInsensitive(List<String> list) {
        logger.log(Level.INFO, "Start to sort collection of strings");
        list.sort(String.CASE_INSENSITIVE_ORDER);
    }

    public void createFileWithSizeSpecified(String filename, int sizeInMb) {
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

    public RandomAccessFile openFileForReading() throws IOException {
        logger.log(Level.INFO, String.format("Trying to get random access to [%s]", pathToInputFile));
        RandomAccessFile randomAccessFile = new RandomAccessFile(pathToInputFile.toFile(), "r");
        logger.log(Level.INFO, "Return random access stream for: " + pathToInputFile);
        return randomAccessFile;
    }

    public boolean closeFileStream(RandomAccessFile randomAccessFile) {
        logger.log(Level.INFO, String.format("Trying to close random access stream to [%s]", pathToInputFile));
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
            return false;
        }
        return true;
    }

    public void savePartStringsInFile(List<String> strings, int part) {
        logger.log(Level.INFO, String.format("Start to write list of strings in [%s]", (part + ".txt")));
        FileWriter writer = null;
        try {
            writer = new FileWriter(String.format("%s.txt", part));
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        for (String str : strings) {
            try {
                writer.write(str + System.lineSeparator());
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        try {
            writer.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public List<String> readPartAsStrings(RandomAccessFile randomAccessFile, int count) {

        boolean lastPart = count == partsCount - 1;

        if (lastPart) {
            try {
                filePointer = randomAccessFile.getFilePointer();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
            buffer = new byte[(int)(fileSize - filePointer)];
        }

        logger.log(Level.INFO, String.format("Read in buffer next [%s] bytes", (partSizeMb * 1024 * 1024)));

        try {
            randomAccessFile.read(buffer);
        } catch (EOFException e) {
            logger.log(Level.INFO, "Reached END of file: " + e.toString());
        }
        catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        strings.clear();

        logger.log(Level.INFO, "Starting convert buffer in strings array list");


        BufferedReader r = null;
        try {
            r = new BufferedReader(
                    new InputStreamReader(
                            new ByteArrayInputStream(buffer), "windows-1251"));
        } catch (UnsupportedEncodingException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        if (r == null) return null;

        try {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                if (line.contains("-1")) break;
                strings.add(line);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        } finally {
            try {
                r.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        logger.log(Level.INFO, "Return strings array");
        return strings;
    }

}
