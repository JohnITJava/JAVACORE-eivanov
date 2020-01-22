package com.gridu.exsort;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.gridu.exsort.ExternalSort.PART_CHUNK_STRINGS_FACTOR;

@Slf4j
@Getter
@NoArgsConstructor
public class FileHandler {
    private static final String SORTED_OUTPUT_FILEPATH = "sortedOutput.txt";

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

    public FileHandler(String inputFilePath, int partSizeMb) {
        this.buffer = new byte[partSizeMb * 1024 * 1024];
        this.pathToInputFile = Paths.get(inputFilePath);
        this.fileSize = pathToInputFile.toFile().length();
        this.partSizeMb = partSizeMb;
        this.partsCount = (int) Math.ceil((double) fileSize / (partSizeMb * 1024 * 1024));
        bafList = new ArrayList<>(partsCount);
        preOutputSortBuffer = new TreeSet<>(MapEntry.compareByValueInsensitiveOrder);
    }

    public void divideIntoSortedParts() {
        log.info("Start divide big file in sorted chunks");
        RandomAccessFile raf = null;

        try {
            raf = openFileForReading();
        } catch (IOException e) {
            log.error(e.toString());
        }

        for (int i = 0; i < partsCount; i++) {
            //TODO last part calculate here and send flag in readPartAsStrings
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
        log.info("Start to sort collection of outputStrings");
        list.sort(String.CASE_INSENSITIVE_ORDER);
    }

    private RandomAccessFile openFileForReading() throws IOException {
        log.info(String.format("Trying to get random access to [%s]", pathToInputFile));
        RandomAccessFile randomAccessFile = new RandomAccessFile(pathToInputFile.toFile(), "r");
        log.info("Return random access stream for: " + pathToInputFile);
        return randomAccessFile;
    }

    private boolean closeFileStream(RandomAccessFile randomAccessFile) {
        log.info(String.format("Trying to close random access stream to [%s]", pathToInputFile));
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            log.error(e.toString());
            return false;
        }
        return true;
    }

    private void savePartStringsInFile(List<String> strings, int part) {
        log.info(String.format("Start to write list of outputStrings in [%s]", (part + ".txt")));
        try (FileWriter writer = new FileWriter(String.format("%s.txt", part))) {
            for (String str : strings) {
                writer.write(str + System.lineSeparator());
            }
            //writer.flush();
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    private List<String> readPartAsStrings(RandomAccessFile randomAccessFile, int count) {

        boolean lastPart = count == partsCount - 1;

        if (lastPart) {
            try {
                filePointer = randomAccessFile.getFilePointer();
            } catch (IOException e) {
                log.error(e.toString());
            }
            buffer = new byte[(int) (fileSize - filePointer)];
        }

        log.info(String.format("Read in buffer next [%s] bytes", (partSizeMb * 1024 * 1024)));

        try {
            randomAccessFile.read(buffer);
        } catch (EOFException e) {
            log.info("Reached END of file: " + e.toString());
        } catch (IOException e) {
            log.error(e.toString());
        }

        //TODO move in separate method
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
                log.error(e.toString());
            }
        }

        //TODO 10Mb in buffer and 10 in list
        outputStrings.clear();

        log.info("Starting convert buffer in outputStrings array list");

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
            log.error(e.toString());
        } finally {
            try {
                r.close();
            } catch (IOException e) {
                log.error(e.toString());
            }
        }

        log.info("Return outputStrings array");
        if ((!lastPart && buffer[buffer.length - 1] != 10)) {
            outputStrings.remove(outputStrings.size() - 1);
        }
        return outputStrings;
    }

    private static boolean checkLineIsEmpty(String line) {
        return line.trim().length() == 0;
    }

    private void mergingIntoOne(String fileOutputPath) {
        bafList = openStreamsForAllParts();
        partsChunksStrings = getFirstStringsArraysFromAllParts(bafList);
        outputStrings.clear();

        endedBafs = new LinkedHashSet<>();
        FileWriter writerOutput = openOutputWriterStream(fileOutputPath);

        //First merging from all string chunks
        firstFillingPreOutputBuffer(partsChunksStrings);

        log.info("While buffered streams are not ended we continue merging");
        while (bafList.size() != endedBafs.size() & !preOutputSortBuffer.isEmpty()) {

            log.info("Begin fill outputStrings while parts are not empty");

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
        log.info("Opening streams for all sorted chunks");

        BufferedReader baf = null;

        for (int i = 0; i < partsCount; i++) {
            String filePath = String.format("%s.txt", i);

            try {
                baf = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                log.error(e.toString());
            }

            bafList.add(baf);
        }
        return bafList;
    }

    private FileWriter openOutputWriterStream(String filePath) {
        log.info("Opening stream for output file: [%s]", filePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(filePath);
        } catch (IOException e) {
            log.error(e.toString());
        }
        return writer;
    }

    private void closeChunksStreams(Set<BufferedReader> bafs) {
        log.info("Closing random access file chunks streams");

        for (BufferedReader baf : bafs) {
            try {
                baf.close();
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
    }

    private void writeStringsOutput(FileWriter writer, List<String> outputList) {
        for (int i = 0; i < outputList.size(); i++) {
            try {
                writer.write(outputList.get(i) + System.lineSeparator());
            } catch (IOException e) {
                log.error(e.toString());
            }
        }

        try {
            writer.flush();
        } catch (IOException e) {
            log.error(e.toString());
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
        log.info("Get first strings arrays from all parts");
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
        log.info("Begin load outputStrings of part in his personal buf array");

        LinkedList<String> partChunk = new LinkedList<>();
        String string = null;

        for (int i = 0; i < partChunkStringsSize; i++) {

            try {
                string = reader.readLine();
            } catch (IOException e) {
                log.error(e.toString());
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

    public void processInternalSorting() {
        divideIntoSortedParts();
        mergingIntoOne(SORTED_OUTPUT_FILEPATH);
    }
}
