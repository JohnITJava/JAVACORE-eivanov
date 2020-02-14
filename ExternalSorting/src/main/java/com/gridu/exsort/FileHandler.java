package com.gridu.exsort;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@NoArgsConstructor
public class FileHandler {

    public void processInternalSorting(String inputPath,
                                       String outputPath,
                                       int maxPartSize,
                                       int partChunkStringFactor) throws OutOfMemoryError {

        DivideFileHandler dfh = new DivideFileHandler();
        DivideFileHandler.DivideHandlerResults dhr = dfh.divideIntoSortedParts(inputPath,
                                                                               maxPartSize,
                                                                               partChunkStringFactor);

        MergeFileHandler mfh = new MergeFileHandler(dhr.getMiniBufferChunkSize());
        mfh.mergingIntoOne(dhr.getPartsCount(), outputPath);
    }
}
