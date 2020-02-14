package com.gridu.exsort;

import lombok.extern.slf4j.Slf4j;
import lombok.var;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

@Slf4j
public class MergeFileHandler {
    private final int miniBufferChunkSize;

    public MergeFileHandler(int miniBufferChunkSize) {
        this.miniBufferChunkSize = miniBufferChunkSize;
    }

    public void mergingIntoOne(int partsCount, String outputPath) {
        List<BufferedReader> buffList = openStreamsForAllParts(partsCount);
        TreeSet<MapEntry<Integer, String>> outputSortedBuffer = new TreeSet<>(MapEntry.compareByValueInsensitiveOrder);
        Map<Integer, LinkedList<String>> chunksMapWithMiniBuffers = getFirstStringsArraysFromAllParts(buffList);

        FileWriter writerOutput = openOutputWriterStream(outputPath);

        //First merging from all string chunks
        firstFillingOutputSortedBuffer(chunksMapWithMiniBuffers, outputSortedBuffer);

        log.info("While buffered streams are not ended we continue merging");
        while (!outputSortedBuffer.isEmpty()) {
            log.info("Begin fill outputStrings while parts are not empty");

            List<String> outputStrings = fillOutputStrings(buffList, outputSortedBuffer, chunksMapWithMiniBuffers);
            //OutputStrings array is full lets write
            if (!outputStrings.isEmpty()) {
                writeStringsOutput(writerOutput, outputStrings);
            }
        }
        closeStreams(buffList, writerOutput);
    }

    private List<String> fillOutputStrings(List<BufferedReader> buffList,
                                           TreeSet<MapEntry<Integer, String>> outputSortedBuffer,
                                           Map<Integer, LinkedList<String>> chunksMapWithMiniBuffers) {
        List<String> outputStrings = new ArrayList<>();
        for (int i = 0; i < miniBufferChunkSize; i++) {
            if (outputSortedBuffer.isEmpty()) {
                break;
            }

            MapEntry<Integer, String> minEntry = outputSortedBuffer.pollFirst();
            outputStrings.add(minEntry.getValue());
            int numNextChunk = minEntry.getKey();

            if (chunksMapWithMiniBuffers.get(numNextChunk).isEmpty()) {
                if (tryFillChunkBuffer(chunksMapWithMiniBuffers, numNextChunk, buffList)) {
                    outputSortedBuffer.add(new MapEntry<>(numNextChunk, chunksMapWithMiniBuffers.get(numNextChunk).pollFirst()));
                }
            } else {
                outputSortedBuffer.add(new MapEntry<>(numNextChunk, chunksMapWithMiniBuffers.get(numNextChunk).pollFirst()));
            }
        }
        return outputStrings;
    }

    private void closeStreams(List<BufferedReader> buffList, FileWriter writerOutput) {
        closeChunksStreams(buffList);
        try {
            writerOutput.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private List<BufferedReader> openStreamsForAllParts(int partsCount) {
        List<BufferedReader> buffList = new ArrayList<>();
        log.info("Opening streams for all sorted chunks");

        BufferedReader buffReader = null;

        for (int i = 0; i < partsCount; i++) {
            String filePath = String.format("%s.txt", i);

            try {
                buffReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                log.error(e.toString());
            }

            buffList.add(buffReader);
        }
        return buffList;
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

    private void closeChunksStreams(List<BufferedReader> buffs) {
        log.info("Closing random access file chunks streams");

        for (BufferedReader buff : buffs) {
            try {
                buff.close();
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
    }

    private void writeStringsOutput(FileWriter writer, List<String> outputList) {
        try {
            for (int i = 0; i < outputList.size(); i++) {
                writer.write(outputList.get(i) + System.lineSeparator());
                writer.flush();
            }
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    private boolean tryFillChunkBuffer(Map<Integer, LinkedList<String>> chunksMapWithMiniBuffers,
                                       int numNextChunk,
                                       List<BufferedReader> buffList) {
        //Get new portion of strings
        var strings = getStringsInMiniBufferOfChunk(buffList.get(numNextChunk));

        //Strings == null if stream for reading has reached end of file
        if (!strings.isEmpty()) {
            chunksMapWithMiniBuffers.put(numNextChunk, strings);
        } else {
            return false;
        }
        return true;
    }

    private void firstFillingOutputSortedBuffer(Map<Integer, LinkedList<String>> chunksMapWithMiniBuffers,
                                                SortedSet<MapEntry<Integer, String>> preOutputSortBuffer) {
        for (int i = 0; i < chunksMapWithMiniBuffers.size(); i++) {
            String value = chunksMapWithMiniBuffers.get(i).pollFirst();
            MapEntry<Integer, String> el = new MapEntry<>(i, value);
            preOutputSortBuffer.add(el);
        }
    }

    /**
     * Getting hashMap with number of part and his buffer outputStrings array
     */
    private Map<Integer, LinkedList<String>> getFirstStringsArraysFromAllParts(List<BufferedReader> buffList) {
        log.info("Get first strings arrays from all parts");
        Map<Integer, LinkedList<String>> chunksMapWithMiniBuffers = new HashMap<>(buffList.size());
        int partCount = 0;

        for (BufferedReader reader : buffList) {
            LinkedList<String> miniBuffer = getStringsInMiniBufferOfChunk(reader);
            chunksMapWithMiniBuffers.put(partCount++, miniBuffer);
        }

        return chunksMapWithMiniBuffers;
    }

    /**
     * Return buffer outputStrings array from definite reader of the part
     */
    private LinkedList<String> getStringsInMiniBufferOfChunk(BufferedReader reader) {
        log.info("Begin load outputStrings of part in his personal buf array");

        LinkedList<String> miniBuffer = new LinkedList<>();
        String string = null;

        for (int i = 0; i < miniBufferChunkSize; i++) {

            try {
                string = reader.readLine();
            } catch (IOException e) {
                log.error(e.toString());
            }

            if (string == null) {
                return miniBuffer;
            } else {
                miniBuffer.add(string);
            }
        }
        return miniBuffer;
    }
}
