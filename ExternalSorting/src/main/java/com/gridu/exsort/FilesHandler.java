package com.gridu.exsort;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;

import static com.gridu.exsort.LoggerHandler.logger;
@Getter
@NoArgsConstructor
public class FilesHandler {
    private Path pathToInputFile;
    private byte[] buffer;
    private int partsCount;
    private int partSizeMb;
    private List<String> outputStrings = new LinkedList<>();
    private Long fileSize;
    private Long filePointer;
    private List<BufferedReader> bafList;
    private int partChunkStringsSize;

    public FilesHandler(String inputFilePath, int partSizeMb) {
        this.buffer = new byte[partSizeMb * 1024 * 1024];
        this.pathToInputFile = Paths.get(inputFilePath);
        this.fileSize = pathToInputFile.toFile().length();
        this.partSizeMb = partSizeMb;
        this.partsCount = (int) Math.ceil((double) fileSize / (partSizeMb * 1024 * 1024));
        bafList = new ArrayList<>(partsCount);
    }

    public static void sortCollectionInsensitive(List<String> list) {
        logger.log(Level.INFO, "Start to sort collection of outputStrings");
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

        outputStrings.clear();

        logger.log(Level.INFO, "Starting convert buffer in outputStrings array list");


        //Convert byte array in buffer as line
        BufferedReader r = new BufferedReader(
                new InputStreamReader(
                        new ByteArrayInputStream(buffer), Charset.defaultCharset()));

        if (r == null) return null;
        String line = "-1";
        try {
            for (line = r.readLine(); line != null; line = r.readLine()) {
                outputStrings.add(line);
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
        return outputStrings;
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
                partChunkStringsSize = (strings.size() * 10) / 100;
            }
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
        try {
            writer.write(string + System.lineSeparator());
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.toString());
        }
    }

    public void mergingIntoOne() {
        List<BufferedReader> bafList = openStreamsForAllParts();
        HashMap<Integer, LinkedList<String>> partsChunksWithStrings = getFirstStringsArraysFromAllParts(bafList);
        outputStrings.clear();

        //String from part of stream via reading it
        String chunkString = null;

        List<BufferedReader> endedBafs = new LinkedList<>();

        //Map contains num of part stream and their string
        Map<Integer, String> chunksBufferStrings = new HashMap<>();
        FileWriter writerOutput = openOutputWriterStream();

        List<String> stringValues = new ArrayList<>();

        //Number of part stream which string will be got output for comparison
        int chunkNext = -1;

        logger.log(Level.INFO, "Begin merging outputStrings while parts are not empty");

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

                //Add in intermediate outputStrings array all chunks outputStrings
                stringValues.addAll(chunksBufferStrings.values());
                chunkNext = extendHelper(chunksBufferStrings, writerOutput, stringValues);

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
                chunkNext = extendHelper(chunksBufferStrings, writerOutput, stringValues);
            }
        }

        closeChunksStreams(endedBafs);
        try {
            writerOutput.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Getting hashMap with number of part and his buffer outputStrings array
     */
    private HashMap<Integer, LinkedList<String>> getFirstStringsArraysFromAllParts(List<BufferedReader> bafList) {
        HashMap<Integer, LinkedList<String>> partsChunksStrings = new HashMap<>(bafList.size());
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

            partChunk.add(string);
        }
        return partChunk;
    }

    //Method of string array sort and write min in file and return number of part containing this string
    private int extendHelper(Map<Integer, String> chunksBufferStrings,
                            FileWriter writerOutput,
                            List<String> stringValues) {
        int chunkNext;
        FilesHandler.sortCollectionInsensitive(stringValues);
        String minString = stringValues.get(0);

        //Save min string in part
        writeStringOutput(writerOutput, minString);

        //Calculate from what part this string
        chunkNext = chunksBufferStrings.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(minString))
                .findFirst()
                .orElseThrow(IllegalStateException::new)
                .getKey();

        chunksBufferStrings.remove(chunkNext);
        return chunkNext;
    }

    public static void createFileWithRandomSymbols() throws IOException {
        File file = new File("generatedInput.txt");
        FileOutputStream fos = new FileOutputStream(file);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        String randomString = null;
        int randomCount = 0;

        while (file.length() < 5000000) {
            randomCount = new Random().nextInt(10) + 1;
            randomString = RandomStringUtils.random(randomCount, true, true);

            bw.write(randomString);
            bw.newLine();
        }

        bw.close();
    }
}
