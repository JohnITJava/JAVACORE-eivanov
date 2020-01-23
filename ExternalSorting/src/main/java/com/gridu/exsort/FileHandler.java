package com.gridu.exsort;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
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

@Slf4j
@Getter
@NoArgsConstructor
public class FileHandler {
    private int partMiniBufferOfStringsSize;

    private int divideIntoSortedParts(String inputPath, int maxPartInMb, int partChunkStringFactor) {
        log.info("Start divide big file in sorted chunks");
        RandomAccessFile raf = null;
        Path pathToInputFile = Paths.get(inputPath);
        Long fileSize = pathToInputFile.toFile().length();
        int partsCount = (int) Math.ceil((double) fileSize / (maxPartInMb * 1024 * 1024));
        boolean isLastPart;

        try {
            raf = openFileForReading(pathToInputFile);
        } catch (IOException e) {
            log.error(e.toString());
        }

        for (int i = 0; i < partsCount; i++) {
            isLastPart = i == partsCount - 1;
            List<String> strings = readPartAsStrings(raf, fileSize, maxPartInMb, isLastPart);
            sortCollectionInsensitive(strings);
            savePartStringsInFile(strings, i);

            //Calculate personal buffer part string array size, just once
            if (i == 0) {
                partMiniBufferOfStringsSize = (strings.size() * partChunkStringFactor) / 100;
            }
        }
        closeFileStream(raf, pathToInputFile);
        return partsCount;
    }

    private static void sortCollectionInsensitive(List<String> list) {
        log.info("Start to sort collection of outputStrings");
        list.sort(String.CASE_INSENSITIVE_ORDER);
    }

    private RandomAccessFile openFileForReading(Path pathToInputFile) throws IOException {
        log.info(String.format("Trying to get random access to [%s]", pathToInputFile));
        RandomAccessFile randomAccessFile = new RandomAccessFile(pathToInputFile.toFile(), "r");
        log.info("Return random access stream for: " + pathToInputFile);
        return randomAccessFile;
    }

    private boolean closeFileStream(RandomAccessFile randomAccessFile, Path pathToInputFile) {
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

    private List<String> readPartAsStrings(RandomAccessFile randomAccessFile,
                                           Long fileSize,
                                           int maxPartInMb,
                                           boolean isLastPart) {

        //We have double expenses since keep bytes and strings in RAM
        byte[] buffer = evaluateBufferSizeForReadingBytesAtOnce(randomAccessFile, fileSize, maxPartInMb, isLastPart);
        List<String> outputStrings = new LinkedList<>();

        log.info(String.format("Read in buffer next [%s] bytes", (buffer.length * 1024 * 1024)));

        try {
            randomAccessFile.read(buffer);
        } catch (IOException e) {
            log.info("Reached END of file: " + e.toString());
        }

        //raf can trim our bytes in middle of string - we calculate back pointer
        moveFilePointerBackIfStringWasDivided(randomAccessFile, buffer, isLastPart);

        log.info("Starting convert buffer in outputStrings array list");

        //Convert byte array in buffer as line
        String line;
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(buffer), Charset.defaultCharset()))) {

            for (line = r.readLine(); line != null; line = r.readLine()) {
                if (!checkLineIsEmpty(line)) {
                    outputStrings.add(line);
                }
            }
        } catch (IOException e) {
            log.error(e.toString());
        }

        removeLastStringIfLastSymbolInBufferIsNotLineBreaker(outputStrings, buffer, isLastPart);

        log.info("Return outputStrings array");
        return outputStrings;
    }

    private void removeLastStringIfLastSymbolInBufferIsNotLineBreaker(List<String> outputStrings,
                                                                      byte[] buffer,
                                                                      boolean isLastPart) {
        if (!isLastPart && buffer[buffer.length - 1] != 10) {
            outputStrings.remove(outputStrings.size() - 1);
        }
    }

    private void moveFilePointerBackIfStringWasDivided(RandomAccessFile randomAccessFile,
                                                       byte[] buffer,
                                                       boolean isLastPart) {
        long bytesCountDueLineBreak = 0;
        if (!isLastPart && buffer[buffer.length - 1] != 10) {
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
    }

    private byte[] evaluateBufferSizeForReadingBytesAtOnce(RandomAccessFile raf,
                                                           Long fileSize,
                                                           int maxPartInMb,
                                                           boolean isLastPart) {
        Long filePointer = null;
        if (isLastPart) {
            try {
                filePointer = raf.getFilePointer();
            } catch (IOException e) {
                log.error(e.toString());
            }
            return new byte[(int) (fileSize - filePointer)];
        } else {
            return new byte[maxPartInMb * 1024 * 1024];
        }
    }

    private static boolean checkLineIsEmpty(String line) {
        return line.trim().length() == 0;
    }

    private void mergingIntoOne(int partsCount, String outputPath) {
        List<BufferedReader> bafList = openStreamsForAllParts(partsCount);
        List<String> outputStrings = new LinkedList<>();
        Set<BufferedReader> endedBafs = new LinkedHashSet<>();
        SortedSet<MapEntry<Integer, String>> preOutputSortBuffer = new TreeSet<>(MapEntry.compareByValueInsensitiveOrder);

        Map<Integer, LinkedList<String>> partsWithMiniBufferStrings = getFirstStringsArraysFromAllParts(bafList,
                                                                                                        endedBafs);
        FileWriter writerOutput = openOutputWriterStream(outputPath);

        //First merging from all string chunks
        firstFillingPreOutputBuffer(partsWithMiniBufferStrings, preOutputSortBuffer);

        log.info("While buffered streams are not ended we continue merging");
        while (bafList.size() != endedBafs.size() & !preOutputSortBuffer.isEmpty()) {

            log.info("Begin fill outputStrings while parts are not empty");

            for (int i = 0; i < partMiniBufferOfStringsSize; i++) {
                MapEntry<Integer, String> minEntry;

                if (preOutputSortBuffer.isEmpty()) {
                    break;
                }

                minEntry = preOutputSortBuffer.first();
                outputStrings.add(minEntry.getValue());
                preOutputSortBuffer.removeIf(e -> e.getKey().equals(minEntry.getKey()));

                //After deleting minimal entry we fill set with new one from parts chunks strings from definite chunk
                fillPreOutputBufferWithNewEntry(partsWithMiniBufferStrings,
                                                minEntry.getKey(),
                                                preOutputSortBuffer,
                                                bafList,
                                                endedBafs);
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

    private List<BufferedReader> openStreamsForAllParts(int partsCount) {
        List<BufferedReader> bafList = new ArrayList<>();
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

    private void fillPreOutputBufferWithNewEntry(Map<Integer, LinkedList<String>> partsChunksStrings,
                                                 int numNextChunk,
                                                 SortedSet<MapEntry<Integer, String>> preOutputSortBuffer,
                                                 List<BufferedReader> bafList,
                                                 Set<BufferedReader> endedBafs) {
        //If array is empty we should fill it with new values
        if (partsChunksStrings.get(numNextChunk).isEmpty()) {

            //If success via filling then ok or return from the method
            if (fillPartChunkWithNewStrings(partsChunksStrings, numNextChunk, bafList, endedBafs)) {
                preOutputSortBuffer.add(new MapEntry<>(numNextChunk, partsChunksStrings.get(numNextChunk).pollFirst()));
            }
        } else {
            preOutputSortBuffer.add(new MapEntry<>(numNextChunk, partsChunksStrings.get(numNextChunk).pollFirst()));
        }
    }

    private boolean fillPartChunkWithNewStrings(Map<Integer, LinkedList<String>> partsChunksStrings,
                                                int numNextChunk,
                                                List<BufferedReader> bafList,
                                                Set<BufferedReader> endedBafs) {
        //Get new portion of strings
        var strings = getStringsInMiniBufferOfPart(bafList.get(numNextChunk), endedBafs);

        //Strings == null if stream for reading has reached end of file
        if (!strings.isEmpty()) {
            partsChunksStrings.put(numNextChunk, strings);
        } else {
            return false;
        }
        return true;
    }

    private void firstFillingPreOutputBuffer(Map<Integer, LinkedList<String>> partsWithMiniBufferStrings,
                                             SortedSet<MapEntry<Integer, String>> preOutputSortBuffer) {
        for (int i = 0; i < partsWithMiniBufferStrings.size(); i++) {
            String value = partsWithMiniBufferStrings.get(i).pollFirst();
            MapEntry<Integer, String> el = new MapEntry<>(i, value);
            preOutputSortBuffer.add(el);
        }
    }

    /**
     * Getting hashMap with number of part and his buffer outputStrings array
     */
    private Map<Integer, LinkedList<String>> getFirstStringsArraysFromAllParts(List<BufferedReader> bafList,
                                                                               Set<BufferedReader> endedBafs) {
        log.info("Get first strings arrays from all parts");
        Map<Integer, LinkedList<String>> partsWithMiniBufferStrings = new HashMap<>(bafList.size());
        int partCount = 0;

        for (BufferedReader reader : bafList) {
            LinkedList<String> miniBufferStringsOfPart = getStringsInMiniBufferOfPart(reader, endedBafs);
            partsWithMiniBufferStrings.put(partCount++, miniBufferStringsOfPart);
        }

        return partsWithMiniBufferStrings;
    }

    /**
     * Return buffer outputStrings array from definite reader of the part
     */
    private LinkedList<String> getStringsInMiniBufferOfPart(BufferedReader reader,
                                                            Set<BufferedReader> endedBafs) {
        log.info("Begin load outputStrings of part in his personal buf array");

        LinkedList<String> partChunk = new LinkedList<>();
        String string = null;

        for (int i = 0; i < partMiniBufferOfStringsSize; i++) {

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

    public void processInternalSorting(String inputPath,
                                       String outputPath,
                                       int maxPartSize,
                                       int partChunkStringFactor ) throws OutOfMemoryError {

        int partsCount = divideIntoSortedParts(inputPath, maxPartSize, partChunkStringFactor);
        mergingIntoOne(partsCount, outputPath);
    }
}
