package com.gridu.exsort;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;

import static com.gridu.exsort.LoggerHandler.logger;

@Data
@NoArgsConstructor
public class FilesHandler {
    private Path pathToInputFile;
    private byte[] buffer;
    private int partsCount;
    private int partSizeMb;
    private List<String> strings = new LinkedList<>();
    private Long fileSize;
    private Long filePointer;
    private List<String> preMergedStringsBuffer;
    private List<BufferedReader> bafList;
    private FileWriter outputWriter;

    public FilesHandler(String inputFilePath, int partSizeMb) {
        this.buffer = new byte[partSizeMb * 1024 * 1024];
        this.pathToInputFile = Paths.get(inputFilePath);
        this.fileSize = pathToInputFile.toFile().length();
        this.partSizeMb = partSizeMb;
        this.partsCount = (int) Math.ceil((double) fileSize / (partSizeMb * 1024 * 1024));
        bafList = new ArrayList<>(partsCount);
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
            if (writer != null) {
                writer.flush();
                writer.close();
            }
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
                //if (line.contains("-1")) break;
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
        }
        closeFileStream(raf);
    }

    public List<BufferedReader> openStreamsForAllParts() {
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

    public FileWriter openOutputWriterStream() {
        logger.log(Level.INFO, "Opening stream for output file: [sortedOutput.txt]");
        FileWriter writer = null;
        try {
            writer = new FileWriter("sortedOutput.txt");
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
        return writer;
    }

    public void closeChunksStreams(List<BufferedReader> bafs) {
        logger.log(Level.INFO, "Closing random access file chunks streams");

        for (BufferedReader baf : bafs) {
            try {
                baf.close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.toString());
            }
        }
    }

    public void writeStringOutput(FileWriter writer, String string) {
        logger.log(Level.INFO, String.format("Writing minimal string in file [%s]", string));
        try {
            writer.write(string + System.lineSeparator());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public void mergingIntoOne() {
        List<BufferedReader> bafList = openStreamsForAllParts();

        String chunkString = null;

        List<BufferedReader> endedBafs = new LinkedList<>();
        Map<Integer, String> chunksBufferStrings = new HashMap<>();
        FileWriter writerOutput = openOutputWriterStream();
        List<String> stringValues = new ArrayList<>();

        int chunkNext = -1;

        while (bafList.size() != endedBafs.size()) {

            if (chunkNext < 0) {
                int i = 0;
                for (BufferedReader baf : bafList) {

                    try {
                        chunkString = baf.readLine();
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, e.toString());
                    }

                    if (chunkString != null) {
                        chunksBufferStrings.put(i++, chunkString);
                    } else {
                        endedBafs.add(baf);
                    }
                }

                stringValues.addAll(chunksBufferStrings.values());
                chunkNext = extendHelper(chunkString, chunksBufferStrings, writerOutput, stringValues);

            } else {
                try {
                    chunkString = bafList.get(chunkNext).readLine();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, e.toString());
                }

                if (chunkString != null) {
                    chunksBufferStrings.put(chunkNext, chunkString);
                } else {
                    endedBafs.add(bafList.get(chunkNext));
                }

                if (chunksBufferStrings.isEmpty()) break;

                stringValues.clear();
                stringValues.addAll(chunksBufferStrings.values());
                chunkNext = extendHelper(chunkString, chunksBufferStrings, writerOutput, stringValues);
            }
        }

        closeChunksStreams(endedBafs);
        try {
            writerOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int extendHelper(String chunkString,
                            Map<Integer, String> chunksBufferStrings,
                            FileWriter writerOutput,
                            List<String> stringValues) {
        int chunkNext;
        FilesHandler.sortCollectionInsensitive(stringValues);
        String minString = stringValues.get(0);

        writeStringOutput(writerOutput, chunkString);

        chunkNext = chunksBufferStrings.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(minString))
                .findFirst()
                .orElseThrow(IllegalStateException::new)
                .getKey();

        chunksBufferStrings.remove(chunkNext);
        return chunkNext;
    }
}
