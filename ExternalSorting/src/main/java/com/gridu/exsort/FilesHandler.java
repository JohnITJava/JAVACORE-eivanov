package com.gridu.exsort;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.var;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;

import static com.gridu.exsort.ExternalSort.PART_CHUNK_STRINGS_FACTOR;
import static com.gridu.exsort.LoggerHandler.logger;

@Getter
@NoArgsConstructor
public class FilesHandler {
    public static final String SORTED_OUTPUT_FILEPATH = "sortedOutput.txt";
    private Path pathToInputFile;
    private byte[] buffer;
    private int partsCount;
    private int partSizeMb;
    private List<String> outputStrings = new LinkedList<>();
    private Long fileSize;
    private Long filePointer;
    private List<BufferedReader> bafList;
    private Set<BufferedReader> endedBafs;
    private int partChunkStringsSize;
    private SortedSet<MapEntry<Integer, String>> preOutputSortBuffer;
    private MapEntry<Integer, String> minEntry;
    private Map<Integer, LinkedList<String>> partsChunksStrings;

    public FilesHandler(String inputFilePath, int partSizeMb) {
        this.buffer = new byte[partSizeMb * 1024 * 1024];
        this.pathToInputFile = Paths.get(inputFilePath);
        this.fileSize = pathToInputFile.toFile().length();
        this.partSizeMb = partSizeMb;
        this.partsCount = (int) Math.ceil((double) fileSize / (partSizeMb * 1024 * 1024));
        bafList = new ArrayList<>(partsCount);
        preOutputSortBuffer = new TreeSet<>(MapEntry.compareByValueInsensitiveOrder);
    }

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

    public void divideIntoSortedParts() {
        logger.log(Level.INFO, "Start divide big file in sorted chunks");
        RandomAccessFile raf = null;

        try {
            raf = openFileForReading();
        } catch (IOException e) {
            logger.log(Level.SEVERE, Arrays.toString(e.getStackTrace()));
        }

        for (int i = 0; i < getPartsCount(); i++) {
            List<String> strings = readPartAsStrings(raf, i);
            sortCollectionInsensitive(strings);
            savePartStringsInFile(strings, i);

            //Calculate personal buffer part string array size, just once
            if (i == 0) {
                partChunkStringsSize = (strings.size() * PART_CHUNK_STRINGS_FACTOR) / 100;
            }
        }
        closeFileStream(raf);
    }

    private static void sortCollectionInsensitive(List<String> list) {
        logger.log(Level.INFO, "Start to sort collection of outputStrings");
        list.sort(String.CASE_INSENSITIVE_ORDER);
    }

    private RandomAccessFile openFileForReading() throws IOException {
        logger.log(Level.INFO, String.format("Trying to get random access to [%s]", pathToInputFile));
        RandomAccessFile randomAccessFile = new RandomAccessFile(pathToInputFile.toFile(), "r");
        logger.log(Level.INFO, "Return random access stream for: " + pathToInputFile);
        return randomAccessFile;
    }

    private boolean closeFileStream(RandomAccessFile randomAccessFile) {
        logger.log(Level.INFO, String.format("Trying to close random access stream to [%s]", pathToInputFile));
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
            return false;
        }
        return true;
    }

    private void savePartStringsInFile(List<String> strings, int part) {
        logger.log(Level.INFO, String.format("Start to write list of outputStrings in [%s]", (part + ".txt")));
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
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    private List<String> readPartAsStrings(RandomAccessFile randomAccessFile, int count) {

        boolean lastPart = count == partsCount - 1;

        if (lastPart) {
            try {
                filePointer = randomAccessFile.getFilePointer();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
            buffer = new byte[(int) (fileSize - filePointer)];
        }

        logger.log(Level.INFO, String.format("Read in buffer next [%s] bytes", (partSizeMb * 1024 * 1024)));

        try {
            randomAccessFile.read(buffer);
        } catch (EOFException e) {
            logger.log(Level.INFO, "Reached END of file: " + e.toString());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        //raf can trim our bytes in middle of string - we calculate back pointer
        long bytesCountDueLineBreak = 0;
        if (!lastPart && buffer[buffer.length - 1] != 10) {
            for (int i = buffer.length - 1; i > 0; i--) {
                bytesCountDueLineBreak++;
                if (buffer[i] == 10) { //10 - byte of line breaker
                    break;
                }
            }

            try {
                randomAccessFile.seek(randomAccessFile.getFilePointer() - bytesCountDueLineBreak);
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        outputStrings.clear();

        logger.log(Level.INFO, "Starting convert buffer in outputStrings array list");


        //Convert byte array in buffer as line
        BufferedReader r = new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(buffer), Charset.defaultCharset()));

        String line;
        try {
            for (line = r.readLine(); line != null; line = r.readLine()) {
                if (!checkLineIsEmpty(line)) {
                    outputStrings.add(line);
                }
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

        logger.log(Level.INFO, "Return outputStrings array");
        if ((!lastPart && buffer[buffer.length - 1] != 10)) {
            outputStrings.remove(outputStrings.size() - 1);
        }
        return outputStrings;
    }

    private static boolean checkLineIsEmpty(String line) {
        return line.trim().length() == 0;
    }

    public void mergingIntoOne(String fileOutputPath) {
        bafList = openStreamsForAllParts();
        partsChunksStrings = getFirstStringsArraysFromAllParts(bafList);
        outputStrings.clear();

        endedBafs = new LinkedHashSet<>();
        FileWriter writerOutput = openOutputWriterStream(fileOutputPath);

        //First merging from all string chunks
        firstFillingPreOutputBuffer(partsChunksStrings);

        logger.log(Level.INFO, "While buffered streams are not ended we continue merging");
        while (bafList.size() != endedBafs.size() & !preOutputSortBuffer.isEmpty()) {

            logger.log(Level.INFO, "Begin fill outputStrings while parts are not empty");

            for (int i = 0; i < partChunkStringsSize; i++) {
                if (preOutputSortBuffer.isEmpty()) {
                    break;
                }

                minEntry = preOutputSortBuffer.first();
                outputStrings.add(minEntry.getValue());
                preOutputSortBuffer.removeIf(e -> e.getKey().equals(minEntry.getKey()));

                //After deleting minimal entry we fill set with new one from parts chunks strings from definite chunk
                fillPreOutputBufferWithNewEntry(partsChunksStrings, minEntry.getKey());
            }

            //OutputStrings array is full lets write
            if (!outputStrings.isEmpty()) {
                writeStringsOutput(writerOutput, outputStrings);
            }
        }

        closeChunksStreams(endedBafs);
        try {
            writerOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<BufferedReader> openStreamsForAllParts() {
        logger.log(Level.INFO, "Opening streams for all sorted chunks");

        BufferedReader baf = null;

        for (int i = 0; i < partsCount; i++) {
            String filePath = String.format("%s.txt", i);

            try {
                baf = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                logger.log(Level.SEVERE, e.toString());
            }

            bafList.add(baf);
        }
        return bafList;
    }

    private FileWriter openOutputWriterStream(String filePath) {
        logger.log(Level.INFO, "Opening stream for output file: [%s]", filePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(filePath);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return writer;
    }

    private void closeChunksStreams(Set<BufferedReader> bafs) {
        logger.log(Level.INFO, "Closing random access file chunks streams");

        for (BufferedReader baf : bafs) {
            try {
                baf.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }
    }

    private void writeStringsOutput(FileWriter writer, List<String> outputList) {
        for (int i = 0; i < outputList.size(); i++) {
            try {
                writer.write(outputList.get(i) + System.lineSeparator());
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }

        try {
            writer.flush();
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }

        outputList.clear();
    }

    private void fillPreOutputBufferWithNewEntry(Map<Integer, LinkedList<String>> partsChunksStrings, int numNextChunk) {
        //If array is empty we should fill it with new values
        if (partsChunksStrings.get(numNextChunk).isEmpty()) {

            //If success via filling then ok or return from the method
            if (fillPartChunkWithNewStrings(partsChunksStrings, numNextChunk)) {
                preOutputSortBuffer.add(new MapEntry<>(numNextChunk, partsChunksStrings.get(numNextChunk).pollFirst()));
            }
        } else {
            preOutputSortBuffer.add(new MapEntry<>(numNextChunk, partsChunksStrings.get(numNextChunk).pollFirst()));
        }
    }

    private boolean fillPartChunkWithNewStrings(Map<Integer, LinkedList<String>> partsChunksStrings, int numNextChunk) {
        //Get new portion of strings
        var strings = getStringsInChunkPart(bafList.get(numNextChunk));

        //Strings == null if stream for reading has reached end of file
        if (!strings.isEmpty()) {
            partsChunksStrings.put(numNextChunk, strings);
        } else {
            return false;
        }
        return true;
    }

    private void firstFillingPreOutputBuffer(Map<Integer, LinkedList<String>> partsChunksStrings) {
        for (int i = 0; i < partsChunksStrings.size(); i++) {
            String value = partsChunksStrings.get(i).pollFirst();
            MapEntry<Integer, String> el = new MapEntry<>(i, value);
            preOutputSortBuffer.add(el);
        }
    }

    /**
     * Getting hashMap with number of part and his buffer outputStrings array
     */
    private Map<Integer, LinkedList<String>> getFirstStringsArraysFromAllParts(List<BufferedReader> bafList) {
        logger.log(Level.INFO, "Get first strings arrays from all parts");
        Map<Integer, LinkedList<String>> partsChunksStrings = new HashMap<>(bafList.size());
        int partCount = 0;

        for (BufferedReader reader : bafList) {

            LinkedList<String> partChunk = getStringsInChunkPart(reader);
            partsChunksStrings.put(partCount++, partChunk);
        }

        return partsChunksStrings;
    }

    /**
     * Return buffer outputStrings array from definite reader of the part
     */
    private LinkedList<String> getStringsInChunkPart(BufferedReader reader) {
        logger.log(Level.INFO, "Begin load outputStrings of part in his personal buf array");

        LinkedList<String> partChunk = new LinkedList<>();
        String string = null;

        for (int i = 0; i < partChunkStringsSize; i++) {

            try {
                string = reader.readLine();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }

            if (string == null) {
                endedBafs.add(reader);
                return partChunk;
            } else {
                partChunk.add(string);
            }
        }
        return partChunk;
    }

    private void eraseLastLine(String filePath) {
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
