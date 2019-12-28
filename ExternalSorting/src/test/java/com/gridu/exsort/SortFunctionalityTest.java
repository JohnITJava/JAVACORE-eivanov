package com.gridu.exsort;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static com.gridu.exsort.ExternalSort.MAX_PART_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SortFunctionalityTest {
    private static final String testInputFilePath = "testInput.txt";
    private static final String testOutputFilePath = "testOutput.txt";

    @Test
    void checkOutputFile_IsSorted() throws IOException {
        FilesHandler.createFileWithRandomSymbols(2, testInputFilePath, 9, false, true);
        List<Integer> inputNums = readAllStringsAsNumsFromFile(testInputFilePath);

        FilesHandler filesHandler = new FilesHandler(testInputFilePath, MAX_PART_SIZE);
        filesHandler.divideIntoSortedParts();
        filesHandler.mergingIntoOne(testOutputFilePath);

        List<Integer> outputNums = readAllStringsAsNumsFromFile(testOutputFilePath);

        for (int i = 0; i < outputNums.size() - 1; i++) {
            if (outputNums.get(i + 1) == null) {
                break;
            }
            assertThat(outputNums.get(i)).isLessThanOrEqualTo(outputNums.get(i));
        }
    }

    private List<Integer> readAllStringsAsNumsFromFile(String fileInputPath) throws IOException {
        File file = new File(fileInputPath);
        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));

        List<Integer> nums = new ArrayList<>();
        String line;
        int num = 0;

        for (line = reader.readLine(); line != null; line = reader.readLine()) {
            try {
                num = Integer.valueOf(line);
                nums.add(num);
            } catch (NumberFormatException e) {
                e.getStackTrace();
            }
        }
        return nums;
    }

    @Test
    void checkOutputFile_ContainsTheSameElementsAsInput() throws IOException {
        FilesHandler.createFileWithRandomSymbols(2, testInputFilePath, 5, false, true);
        List<Integer> inputNums = readAllStringsAsNumsFromFile(testInputFilePath);

        FilesHandler filesHandler = new FilesHandler(testInputFilePath, MAX_PART_SIZE);
        filesHandler.divideIntoSortedParts();
        filesHandler.mergingIntoOne(testOutputFilePath);

        List<Integer> outputNums = readAllStringsAsNumsFromFile(testOutputFilePath);

        assertThat(outputNums).hasSameSizeAs(inputNums);
        assertThat(outputNums).containsAll(inputNums);
    }

    @Test
    void checkOutputStrings_SortedDueCaseInsensitiveOrder() throws IOException {
        FilesHandler.createFileWithRandomSymbols(5, testInputFilePath, 9, true, false);
        List<String> inputStrings = readAllStringsFromFile(testInputFilePath);

        FilesHandler filesHandler = new FilesHandler(testInputFilePath, MAX_PART_SIZE);
        filesHandler.divideIntoSortedParts();
        filesHandler.mergingIntoOne(testOutputFilePath);

        List<String> outputStrings = readAllStringsFromFile(testOutputFilePath);

        assertThat(outputStrings).isSortedAccordingTo(String.CASE_INSENSITIVE_ORDER);
    }

    private List<String> readAllStringsFromFile(String fileInputPath) throws IOException {
        File file = new File(fileInputPath);
        FileInputStream fileInputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));

        List<String> strings = new ArrayList<>();
        String line;

        for (line = reader.readLine(); line != null; line = reader.readLine()) {
            strings.add(line);
        }
        return strings;
    }
}
