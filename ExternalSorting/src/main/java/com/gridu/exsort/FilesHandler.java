package com.gridu.exsort;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;

@Data
@NoArgsConstructor
public class FilesHandler {
    private Path pathToInputFile;

    public FilesHandler(String inputFilePath) {
        pathToInputFile = Paths.get(inputFilePath);
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
}
