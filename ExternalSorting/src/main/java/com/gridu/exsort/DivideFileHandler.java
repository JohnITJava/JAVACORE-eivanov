package com.gridu.exsort;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Getter
@NoArgsConstructor
public class DivideFileHandler {

    public DivideHandlerResults divideIntoSortedParts(String inputPath, int maxPartInMb, int partChunkStringFactor) {
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

        int miniBufferChunkSize = 0;
        for (int i = 0; i < partsCount; i++) {
            isLastPart = i == partsCount - 1;
            List<String> strings = readPartAsStrings(raf, fileSize, maxPartInMb, isLastPart);
            sortCollectionInsensitive(strings);
            savePartStringsInFile(strings, i);

            //Calculate personal buffer part string array size, just once
            if (i == 0) {
                miniBufferChunkSize = (strings.size() * partChunkStringFactor) / 100;
            }
        }
        closeFileStream(raf, pathToInputFile);
        return new DivideHandlerResults(partsCount, miniBufferChunkSize);
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

        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(buffer), Charset.defaultCharset()))) {

            randomAccessFile.read(buffer);

            //raf can trim our bytes in middle of string - we calculate back pointer
            moveFilePointerBackIfStringWasDivided(randomAccessFile, buffer, isLastPart);

            log.info("Starting convert buffer in outputStrings array list");

            //Convert byte array in buffer as line
            String line;
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

    @AllArgsConstructor
    @Getter
    public class DivideHandlerResults {
        private final int partsCount;
        private final int miniBufferChunkSize;
    }
}
