#include "rbfm.h"

//////////// rbfm_helpers Definitions ////////////

int rbfm_helpers::getActualByteForNullsIndicator(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}


RC rbfm_helpers::recordToData(const std::vector <Attribute> &recordDescriptor, const byte *record, void *data) {

    /* Copy the number of fields from the record */
    unsigned int numberOfFields = 0;
    memcpy(&numberOfFields, record, 4);

    /* Recall that a pointing record has format: [ 11111111 (1 byte) | PageNum | SlotNum ] */
    unsigned char checkPointingRecord = 0;
    memcpy(&checkPointingRecord, record, 1);

    /* If the record is pointing another record, return 2 */
    if(checkPointingRecord == 255){
        //std::cout << "This is a pointing record" << std::endl;
        return 2;
    }

    /* Copy the null field indicator bytes from the record */
    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator((int)numberOfFields);

    /* Copy the offset values into the fieldOffset array */
    auto *fieldOffset = new unsigned[numberOfFields];
    memcpy(fieldOffset, record + 4 + nullFieldsIndicatorActualSize, 4 * numberOfFields);

    /* Copy the field values into the data without NULL values */
    /* Recall that data format is [null_indicator | value_1 | value_2 | ... | value_n ] */
    unsigned int sizeOfField = 0;
    /* Copy null indicator into data */
    memcpy(data, record + 4, nullFieldsIndicatorActualSize);

    byte *dataPtrByte = (byte*)data;
    const unsigned int valueStartOffset = 4 + nullFieldsIndicatorActualSize + 4 * numberOfFields;
    unsigned int dataPtrOffset = nullFieldsIndicatorActualSize;
    unsigned int recordValuePtrOffset = valueStartOffset;

    int nullBitIter = 4;        // Iterator for byte-null-indicator
    unsigned mask = 7;          // Bitwise mask, starts at 7
    bool nullBit = false;
    for(unsigned n = 0; n < numberOfFields; n++){

        /* null field check */
        if ((n != 0) && (n % 8 == 0)) { nullBitIter++; mask = 7; }
        nullBit = record[nullBitIter] & 1u << mask--;

        if (nullBit) {
            continue;
        }

        const Attribute &attr = recordDescriptor[n];
        if(attr.type == TypeVarChar){
            if(n==0){
                sizeOfField = fieldOffset[n] - valueStartOffset;

                memcpy(dataPtrByte + dataPtrOffset, &sizeOfField, 4);
                dataPtrOffset += 4;

                memcpy( dataPtrByte + dataPtrOffset, record + recordValuePtrOffset, sizeOfField );
                dataPtrOffset += sizeOfField;
                recordValuePtrOffset += sizeOfField;
            }else{
                sizeOfField = fieldOffset[n] - fieldOffset[n-1];

                memcpy(dataPtrByte + dataPtrOffset, &sizeOfField, 4);
                dataPtrOffset += 4;

                memcpy( dataPtrByte + dataPtrOffset, record + recordValuePtrOffset, sizeOfField );
                dataPtrOffset += sizeOfField;
                recordValuePtrOffset += sizeOfField;
            }
        }else if(attr.type == TypeInt){
            memcpy(dataPtrByte + dataPtrOffset, record + recordValuePtrOffset, 4 );
            dataPtrOffset += 4;
            recordValuePtrOffset += 4;
        }else if(attr.type == TypeReal){
            memcpy(dataPtrByte + dataPtrOffset, record + recordValuePtrOffset, 4 );
            dataPtrOffset += 4;
            recordValuePtrOffset += 4;
        }
    }

    /* Clean up memory */
    delete [] fieldOffset;

    return 0;

}


RC rbfm_helpers::findNextRecordSlot(byte *pagePtr, unsigned short nextRecordOffset, unsigned short &nextRecordLength,
                                    unsigned short &foundSlotNo) {

    unsigned short N = 0, tempOffset = 0;
    int offset = PAGE_SIZE - 4;
    memcpy(&N, pagePtr + offset, 2);

    unsigned short slotNo = 0;
    for (; slotNo < N; slotNo++){
        offset -= 4;
        memcpy(&tempOffset, pagePtr + offset, 2);
        if(tempOffset == nextRecordOffset){
            memcpy(&nextRecordLength, pagePtr + offset + 2, 2);
            foundSlotNo = slotNo;
            return 0;
        }
    }

    nextRecordLength = 0;
    foundSlotNo = N;
    return 1;
}


RC rbfm_helpers::dataToRecord(const std::vector<Attribute> &recordDescriptor, const void *data, byte *destination) {
    /* Start memory transfer
     * data -> pageBuffer */
    const unsigned int numOfFields = recordDescriptor.size();
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator((int)numOfFields);

    /* Temporary pointer to input data block pointer */
    const byte* inputDataPtr = (byte*)data;

    /* Initialize offsets for the record */
    unsigned int inputOffset = 0 + nullFieldsIndicatorActualSize;        // Offset for navigating input data
    unsigned short relativeRecordValueOffset =  4                     // sizeof(numOfFields)
                                                + nullFieldsIndicatorActualSize
                                                + (numOfFields * 4);
    unsigned short recordPrefixWriterOffset = 0;            // Offset for record prefix (n, nulls, offsets)
    unsigned short recordValueWriterOffset = relativeRecordValueOffset; // Offset for record values

    /* Copy n */
    memcpy(destination + recordPrefixWriterOffset, &numOfFields, 4);
    recordPrefixWriterOffset += 4;

    /* Copy null bytes */
    memcpy(destination + recordPrefixWriterOffset, inputDataPtr, nullFieldsIndicatorActualSize);
    recordPrefixWriterOffset += nullFieldsIndicatorActualSize;

    /* Print each data field for the given record */
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    unsigned mask = 7;          // Bitwise mask, starts at 7
    for (unsigned i=0; i<numOfFields; i++) {
        const Attribute &attr = recordDescriptor[i];
        /* Check null bit */
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; mask = 7; }
        nullBit = inputDataPtr[nullBitIter] & 1u << mask--;

        if (nullBit) {
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);
            recordPrefixWriterOffset += 4;
            continue;
        }

        /* Switch on attribute type */
        if (attr.type == TypeInt) {
            relativeRecordValueOffset += 4;         // Advance relative record value offset to point to end
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, 4);

            recordValueWriterOffset += 4;
            inputOffset += 4;
        } else if (attr.type == TypeReal) {
            relativeRecordValueOffset += 4;         // Advance relative record value offset to point to end
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, 4);

            recordValueWriterOffset += 4;
            inputOffset += 4;
        } else if ((attr.type == TypeVarChar)) {
            unsigned int valLen = 0;

            memcpy(&valLen, inputDataPtr + inputOffset, 4);
            inputOffset += 4;      // should be 4

            relativeRecordValueOffset += valLen;         // Advance relative record value offset
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, valLen);
            recordValueWriterOffset += valLen;
            inputOffset += valLen;
        }
    }

    return 0;
}

RC rbfm_helpers::dataToRecordWithSize(const std::vector<Attribute> &recordDescriptor, const void *data, byte *destination, int* size) {
    /* Start memory transfer
     * data -> pageBuffer */
    const unsigned int numOfFields = recordDescriptor.size();
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator((int)numOfFields);

    /* Temporary pointer to input data block pointer */
    const byte* inputDataPtr = (byte*)data;

    /* Initialize offsets for the record */
    unsigned int inputOffset = 0 + nullFieldsIndicatorActualSize;        // Offset for navigating input data
    unsigned short relativeRecordValueOffset =  4                     // sizeof(numOfFields)
                                                + nullFieldsIndicatorActualSize
                                                + (numOfFields * 4);
    unsigned short recordPrefixWriterOffset = 0;            // Offset for record prefix (n, nulls, offsets)
    unsigned short recordValueWriterOffset = relativeRecordValueOffset; // Offset for record values

    /* Copy n */
    memcpy(destination + recordPrefixWriterOffset, &numOfFields, 4);
    recordPrefixWriterOffset += 4;

    /* Copy null bytes */
    memcpy(destination + recordPrefixWriterOffset, inputDataPtr, nullFieldsIndicatorActualSize);
    recordPrefixWriterOffset += nullFieldsIndicatorActualSize;

    /* Print each data field for the given record */
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    unsigned mask = 7;          // Bitwise mask, starts at 7
    for (unsigned i=0; i<numOfFields; i++) {
        const Attribute &attr = recordDescriptor[i];
        /* Check null bit */
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; mask = 7; }
        nullBit = inputDataPtr[nullBitIter] & 1u << mask--;

        if (nullBit) {
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);
            recordPrefixWriterOffset += 4;
            continue;
        }

        /* Switch on attribute type */
        if (attr.type == TypeInt) {
            relativeRecordValueOffset += 4;         // Advance relative record value offset to point to end
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, 4);

            recordValueWriterOffset += 4;
            inputOffset += 4;
        } else if (attr.type == TypeReal) {
            relativeRecordValueOffset += 4;         // Advance relative record value offset to point to end
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, 4);

            recordValueWriterOffset += 4;
            inputOffset += 4;
        } else if ((attr.type == TypeVarChar)) {
            unsigned int valLen = 0;

            memcpy(&valLen, inputDataPtr + inputOffset, 4);
            inputOffset += 4;      // should be 4

            relativeRecordValueOffset += valLen;         // Advance relative record value offset
            unsigned int valueOffset = relativeRecordValueOffset;
            memcpy(destination + recordPrefixWriterOffset, &valueOffset, 4);   // Write to prefix offset
            recordPrefixWriterOffset += 4;

            /* Copy actual value to page record */
            memcpy(destination + recordValueWriterOffset, inputDataPtr + inputOffset, valLen);
            recordValueWriterOffset += valLen;
            inputOffset += valLen;
        }
    }

    *size = recordValueWriterOffset;

    return 0;
}

unsigned rbfm_helpers::getPredictedRecordSize(const std::vector<Attribute> &recordDescriptor,
                                         const void *data) {
    unsigned retVal = 0;
    int numOfFields = recordDescriptor.size();

    retVal += 4;        // for number of fields

    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator(numOfFields);
    retVal += nullFieldsIndicatorActualSize;        // size of null fields indicator

    int numOfOffsets = numOfFields * 4;
    retVal += numOfOffsets;     // number of offsets

    unsigned offset = 0 + nullFieldsIndicatorActualSize;

    /* Make a pass to see how many bytes are needed to be allocated for the record values */
    bool nullBit = false;
    const byte* dataPtr = (byte*)data;
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    unsigned mask = 7;          // Bitwise mask, starts at 7
    for (int i=0; i<numOfFields; i++) {
        const Attribute &attr = recordDescriptor[i];
        /* Check null bit */
        if ((i != 0) && (i % 8 == 0)) {
            nullBitIter++; mask = 7;
        }
        nullBit = dataPtr[nullBitIter] & (1u << mask);
        mask -= 1;

        if (nullBit) { continue; }

        /* Switch on attribute type */
        if ((attr.type == TypeInt)) {
            retVal += sizeof(int);
            offset += sizeof(int);      // should be 4
        } else if ((attr.type == TypeReal)) {
            retVal += sizeof(float);
            offset += sizeof(float);      // should be 4
        } else if ((attr.type == TypeVarChar)) {
            int valLen = 0;

            memcpy(&valLen, dataPtr + offset, sizeof(int));  // should be 4

            offset += sizeof(int);      // should be 4
            retVal += valLen;
            offset += valLen;       // advance offset by length of string
        }
    }

    return retVal;
}

RC rbfm_helpers::memcpyRecordPointer(byte *memPtr, const RID &rid) {
    const byte PTR_INDICATOR = 255;

    memcpy(memPtr, &PTR_INDICATOR, 1);
    memcpy(memPtr+1, &rid.pageNum, 4);
    memcpy(memPtr+5, &rid.slotNum, 4);

    return 0;
}

RC rbfm_helpers::shiftRecordsToLeft(byte *pagePtr, const unsigned short startOffset, unsigned short nextRecordOffset) {
    // Error-handling
    if (startOffset > nextRecordOffset) {
        //std::cerr << "Starting offset should be less than next!" << std::endl;
        return 1;
    }

    /* Do nothing if both are the same */
    if (startOffset == nextRecordOffset) {
        return 0;
    }

    unsigned short N = 0;
    memcpy(&N, pagePtr + (PAGE_SIZE - 4), 2);

    unsigned short overwriteOffset = startOffset;
    unsigned short nextRecordLength = 0;
    unsigned short nextSlotNo = N;

    /* Shift all the next records to the left */
    RC rc = rbfm_helpers::findNextRecordSlot(pagePtr, nextRecordOffset, nextRecordLength, nextSlotNo);
    while(!rc) {
        /* memmove since pointers can be overlapping */
        memmove(pagePtr + overwriteOffset, pagePtr + nextRecordOffset, nextRecordLength);

        /* modify slot for moved record */
        if (nextSlotNo >= N) {      // Error guard
            //std::cerr << "FATAL ERROR AT shiftRecordToLeft!!" << std::endl;
            return -1;       // Fatal error
        }
        writeDetailsToSlotNo(pagePtr, nextSlotNo, overwriteOffset, nextRecordLength);

        /* Advance offsets */
        overwriteOffset += nextRecordLength;
        nextRecordOffset = nextRecordOffset + nextRecordLength;

        rc = rbfm_helpers::findNextRecordSlot(pagePtr, nextRecordOffset, nextRecordLength, nextSlotNo);
    }

    return 0;
}

RC rbfm_helpers::getDetailsFromSlotNo(byte *pagePtr, const unsigned slotNum,
                                        unsigned short *recordOffset, unsigned short *recordLength) {
    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (slotNum+1) );

    unsigned short to = 0, tl = 0;
    memcpy(&to, pagePtr + nSlotOffset, 2);
    memcpy(&tl, pagePtr + nSlotOffset + 2, 2);

    *recordOffset = to;
    *recordLength = tl;

    return 0;
}

RC rbfm_helpers::writeDetailsToSlotNo(byte *pagePtr, const unsigned slotNum, const unsigned short recordOffset,
                                      const unsigned short recordLength) {
    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (slotNum+1) );

    memcpy(pagePtr + nSlotOffset, &recordOffset,2);
    memcpy(pagePtr + nSlotOffset + 2, &recordLength, 2);

    return 0;
}

unsigned short rbfm_helpers::getRemainingBytesFromPage(byte *pagePtr) {
    unsigned short retVal = 0;
    memcpy(&retVal, pagePtr + (PAGE_SIZE-2), 2);
    return retVal;
}

unsigned short rbfm_helpers::getNumberOfSlotEntriesFromPage(byte *pagePtr) {
    unsigned short retVal = 0;
    memcpy(&retVal, pagePtr + (PAGE_SIZE-4), 2);
    return retVal;
}

RC rbfm_helpers::reformatPage(byte *pagePtr) {
    /* Set memory block to all zeroes */
    memset(pagePtr, 0, PAGE_SIZE);

    unsigned short ushortF_t = PAGE_SIZE - 4;
    unsigned short ushortN_t = 0;

    memcpy(pagePtr + (PAGE_SIZE-2), &ushortF_t, 2);
    memcpy(pagePtr + (PAGE_SIZE-4), &ushortN_t, 2);

    return 0;
}

RC rbfm_helpers::appendRecordToPage(const std::vector<Attribute> &recordDescriptor, const void *data, byte *pagePtr,
                                    unsigned *slotNum, const unsigned short REM_BYTES,
                                    const unsigned short NUM_SLOT_ENTRIES) {
    /* Initialize offsets for the record */
    const unsigned short PAGE_OFFSET = PAGE_SIZE - REM_BYTES -  4 - (4 * NUM_SLOT_ENTRIES);

    /* Transfer data to record */
    int size = 0;
    RC rc = rbfm_helpers::dataToRecordWithSize(recordDescriptor, data, pagePtr + PAGE_OFFSET, &size);
    if (rc != 0) return rc;

    /* Find new slot# */
    bool isSlotReused = false;
    unsigned short newSlotOffset = (unsigned short)PAGE_SIZE - 8;
    for (unsigned short i = 0; i < NUM_SLOT_ENTRIES; ++i) {
        unsigned short slotRecordLen = 0;
        memcpy(&slotRecordLen, pagePtr + (newSlotOffset + 2), 2);
        if (slotRecordLen == 0) {
            isSlotReused = true;   // 0 means slot is reused
            *slotNum = i;
            break;
        } else newSlotOffset -= 4;
    }

    /* Copy slot offset first */
    memcpy(pagePtr + newSlotOffset, &PAGE_OFFSET, 2);
    newSlotOffset += 2;

    /* Copy record length */
    unsigned short recordSize = size;
    memcpy(pagePtr + newSlotOffset, &recordSize, 2);

    /* Update F, N. */
    unsigned short remainingBytes = REM_BYTES;
    unsigned short numberOfSlotEntries = NUM_SLOT_ENTRIES;

    if (isSlotReused) {
        remainingBytes = remainingBytes - recordSize;
    } else {
        numberOfSlotEntries += 1;
        remainingBytes = remainingBytes - recordSize - 4;     // 4 is for the extra slot#
        *slotNum = (numberOfSlotEntries - 1);      // Appended slot
    }


    memcpy(pagePtr + (PAGE_SIZE-2), &remainingBytes, 2);
    memcpy(pagePtr + (PAGE_SIZE-4), &numberOfSlotEntries, 2);

    return 0;
}

RC rbfm_helpers::readRecordFromPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const RID &rid, const byte *page, void *data) {

    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "Can't open input file" << std::endl;
        return 1;
    }

    /* Copy the offset and length values of the record from slot directory */
    unsigned short recordOffset = 0, recordLength = 0;
    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (rid.slotNum+1) );
    memcpy(&recordOffset, page + nSlotOffset, 2);
    memcpy(&recordLength, page + nSlotOffset + 2, 2);

    /* Check if the slot is "dead" (i.e. the corresponding record is deleted ) */
    if(recordLength == 0){
        //std::cerr << "No record! RID(PageNum: " << rid.pageNum << ", SlotNum: " << rid.slotNum << ")" << std::endl;
        return 1;
    }

    /* Copy the record from the page */
    byte* record = new byte[recordLength];
    memcpy(record, page + recordOffset, recordLength);

    /* Convert byte array record to void* data */
    RC result = rbfm_helpers::recordToData(recordDescriptor, record, data);

    if(result == 2){ // In this case, the record is pointing the other record.

        /* If the record is pointing another record, then it includes follows: [ 11111111 (1 byte) | PageNum | slotNum ]
        * So we need to reassign the real record in the other page and continue reading with the real record. */

        unsigned int newPageNum, newSlotNum;
        memcpy(&newPageNum, record + 1, 4);
        memcpy(&newSlotNum, record + 5, 4);

        RID newRid;
        newRid.pageNum = newPageNum;
        newRid.slotNum = newSlotNum;

        // Use recursion to read the real record
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        result = rbfm.readRecord(fileHandle, recordDescriptor, newRid, data);
    }

    if(result != 0){ //if the result is still non-zero, some error happened.
        //std::cerr << "Converting record to data error" << std::endl;
        return 1;
    }

    delete[] record;
    return 0;
}

RC rbfm_helpers::deleteRecordFromPage(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, byte *page) {

    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "Can't open input file" << std::endl;
        return 1;
    }

    /* Copy the offset and length values of the record from slot directory */
    unsigned short recordOffset = 0, recordLength = 0;

    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (rid.slotNum+1) );
    memcpy(&recordOffset, page + nSlotOffset, 2);
    memcpy(&recordLength, page + nSlotOffset + 2, 2);

    unsigned short nextRecordOffset = recordOffset + recordLength;

    /* Check if the slot is "dead" (i.e. the corresponding record is deleted ) */
    if(recordLength == 0){
        //std::cerr << "No record! RID(PageNum: " << rid.pageNum << ", SlotNum: " << rid.slotNum << ")" << std::endl;
        return 7;
    }

    // variables for the case that the record is a pointing record
    RID newRid;

    /* See if the record is pointing another record */
    unsigned char checkPointingRecord = 0;
    memcpy(&checkPointingRecord, page + recordOffset, 1);
    if(checkPointingRecord == 255) { // In this case, the record is pointing the other record.

        /* If the record is pointing the real record, then it includes : [11111111 (=255) (1 byte) | PageNum | slotNum ]
         * So we need to read these values, go to that page, and delete the real record in the page. */

        memcpy(&newRid.pageNum, page + recordOffset + 1, 4);
        memcpy(&newRid.slotNum, page + recordOffset + 5, 4);

        // Use recursion to delete the real record
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RC rc = rbfm.deleteRecord(fileHandle, recordDescriptor, newRid);
    }

    /* Shifting records effectively deletes the record */
    rbfm_helpers::shiftRecordsToLeft(page, recordOffset, nextRecordOffset);

    /* FIXME: Update F and N */
    /* Update F */
    unsigned short F = rbfm_helpers::getRemainingBytesFromPage(page);
    //unsigned short N = rbfm_helpers::getNumberOfSlotEntriesFromPage(page);
    F = F - recordLength;
    //N = N - 1;
    memcpy(page + PAGE_SIZE - 2, &F, 2);
    //memcpy(page + PAGE_SIZE - 4, &N, 2);

    /* Set the slot directory for the deleted record to be "Dead"
     * i.e. Set the recordLength as 0 and offset to PAGE_SIZE */
    rbfm_helpers::writeDetailsToSlotNo(page, rid.slotNum, PAGE_SIZE, 0);

    /* Write the page to the file */
    RC rc = fileHandle.writePage(rid.pageNum, page);
    if(rc){
        //std::cerr << "writing page error" << std::endl;
    }

    return 0;
}

RC rbfm_helpers::stdString2RawValue(const std::string& originalStr, byte *data) {
    int strLen = originalStr.size();
    int offset = 0;

    memcpy(data + offset, &strLen, 4);
    offset += 4;

    for (int i = 0; i < strLen; ++i) {
        byte buf = (byte) originalStr[i];
        memcpy(data + offset, &buf, 1);
        offset += 1;
    }

    return 0;
}

std::string rbfm_helpers::rawValue2StdString(const byte *data) {
    int strLen = 0;
    int offset = 0;
    std::string originalStr;

    memcpy(&strLen, data, 4);
    offset += 4;

    for (int i = 0; i < strLen; ++i) {
        originalStr += data[i+offset];
    }

    return originalStr;
}

//////////// RecordBasedFileManager Definitions ////////////

RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    PagedFileManager &pfm = PagedFileManager::instance();
    return pfm.createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    PagedFileManager &pfm = PagedFileManager::instance();
    return pfm.destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    PagedFileManager &pfm = PagedFileManager::instance();
    return pfm.openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager &pfm = PagedFileManager::instance();
    return pfm.closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    /* Do a linear search over all the current pages in the file, get the page with the available bytes */
    unsigned numOfPages = fileHandle.getNumberOfPages();
    bool availablePageFound = false;

    //RBFM_Page *currentPagePtr = nullptr;
    byte* pageBuffer = new byte[PAGE_SIZE];     // Allocate memory for page buffer
    unsigned curPageNo = 0;
    for (; curPageNo < numOfPages; curPageNo++) {
        /* Get page */
        RC rc = fileHandle.readPage(curPageNo, pageBuffer);
        if (rc != 0) {
            //std::cerr << "Error in readPage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }

        /* Check if remaining bytes exceed the record length */
        auto remainingPageBytes = rbfm_helpers::getRemainingBytesFromPage(pageBuffer);
        auto safeRecordSize = rbfm_helpers::getPredictedRecordSize(recordDescriptor, data) + 4;     // Overestimate

        if (remainingPageBytes > safeRecordSize) {
            /* Break out of the loop if page is found */
            availablePageFound = true;
            break;
        }
    }

    if (availablePageFound) {
        /* If there is an available page in the file
         * Write the record to the buffered page */
        RC rc = rbfm_helpers::appendRecordToPage(recordDescriptor, data, pageBuffer, &rid.slotNum,
                                                 rbfm_helpers::getRemainingBytesFromPage(pageBuffer),
                                                 rbfm_helpers::getNumberOfSlotEntriesFromPage(pageBuffer));
        if (rc != 0) {
            //std::cerr << "Error in appendRecordToPage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }

        /* Write page */
        rc = fileHandle.writePage(curPageNo, pageBuffer);
        if (rc != 0) {
            //std::cerr << "Error in writePage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }

        rid.pageNum = curPageNo;
    } else {
        /* If there are no available pages in the file
         * (All pages are full or no pages) */
        //currentPagePtr = new RBFM_Page(pageBuffer, newPageNo, false);
        /* Format page */
        RC rc = rbfm_helpers::reformatPage(pageBuffer);
        if (rc != 0) {
            //std::cerr << "Error in reformatPage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }

        /* Write the input record to the newly formatted page */
        rc = rbfm_helpers::appendRecordToPage(recordDescriptor, data, pageBuffer, &rid.slotNum,
                                                 rbfm_helpers::getRemainingBytesFromPage(pageBuffer),
                                                 rbfm_helpers::getNumberOfSlotEntriesFromPage(pageBuffer));
        //currentPagePtr->packAndSaveRecordToInternalPagePtr(recordDescriptor, data, rid.slotNum);
        if (rc != 0) {
            //std::cerr << "Error in appendRecordToPage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }

        /* Append page */
        rc = fileHandle.appendPage(pageBuffer);
        if (rc != 0) {
            //std::cerr << "Error in appendPage(), pageNum: " << curPageNo << std::endl;
            return 1;
        }
        rid.pageNum = curPageNo;
    }

    /* Clean up memory */
    delete[] pageBuffer;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {

    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "Can't open input file" << std::endl;
        return 1;
    }

    /* Read the page that includes the record from the file */
    byte *pagePtr = (byte*)malloc(PAGE_SIZE);
    int readPageResult = fileHandle.readPage(rid.pageNum, pagePtr);
    if(readPageResult != 0){
        //std::cout << "read page error" <<std::endl;
        free(pagePtr);
        return 1;
    }

    /* Copy the offset and length values of the record from slot directory */
    unsigned short recordOffset = 0, recordLength = 0;
    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (rid.slotNum+1) );
    memcpy(&recordOffset, pagePtr + nSlotOffset, 2);
    memcpy(&recordLength, pagePtr + nSlotOffset + 2, 2);

    /* Check if the slot is "dead" (i.e. the corresponding record is deleted ) */
    if(recordLength == 0){
        //std::cerr << "No record! RID(PageNum: " << rid.pageNum << ", SlotNum: " << rid.slotNum << ")" << std::endl;
        free(pagePtr);
        return 1;
    }

    /* Copy the record from the page */
    byte* record = new byte[recordLength];
    memcpy(record, pagePtr + recordOffset, recordLength);

    /* Convert byte array record to void* data */
    RC result = rbfm_helpers::recordToData(recordDescriptor, record, data);

    if(result == 2){ // In this case, the record is pointing the other record.

        /* If the record is pointing another record, then it includes follows: [ 11111111 (1 byte) | PageNum | slotNum ]
        * So we need to reassign the real record in the other page and continue reading with the real record. */

        unsigned int newPageNum, newSlotNum;
        memcpy(&newPageNum, record + 1, 4);
        memcpy(&newSlotNum, record + 5, 4);

        RID newRid;
        newRid.pageNum = newPageNum;
        newRid.slotNum = newSlotNum;

        // Use recursion to read the real record
        result = readRecord(fileHandle, recordDescriptor, newRid, data);
    }

    if(result != 0){ //if the result is still non-zero, some error happened.
        //std::cerr << "Converting record to data error" << std::endl;
        free(pagePtr);
        return 1;
    }

    delete[] record;
    free(pagePtr);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {

    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "Can't open input file" << std::endl;
        return 1;
    }

    /* Read the page that includes the record from the file */
    byte *pagePtr = (byte*)malloc(PAGE_SIZE);
    int readPageResult = fileHandle.readPage(rid.pageNum, pagePtr);
    if(readPageResult != 0){
        //std::cout << "read page error" <<std::endl;
        return 1;
    }

    /* Copy the offset and length values of the record from slot directory */
    unsigned short recordOffset = 0, recordLength = 0;

    unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (rid.slotNum+1) );
    memcpy(&recordOffset, pagePtr + nSlotOffset, 2);
    memcpy(&recordLength, pagePtr + nSlotOffset + 2, 2);

    unsigned short nextRecordOffset = recordOffset + recordLength;

    /* Check if the slot is "dead" (i.e. the corresponding record is deleted ) */
    if(recordLength == 0){
        //std::cerr << "No record! RID(PageNum: " << rid.pageNum << ", SlotNum: " << rid.slotNum << ")" << std::endl;
        free(pagePtr);
        return 1;
    }

    // variables for the case that the record is a pointing record
    RID newRid;

    /* See if the record is pointing another record */
    unsigned char checkPointingRecord = 0;
    memcpy(&checkPointingRecord, pagePtr + recordOffset, 1);
    if(checkPointingRecord == 255) { // In this case, the record is pointing the other record.

        /* If the record is pointing the real record, then it includes : [11111111 (=255) (1 byte) | PageNum | slotNum ]
         * So we need to read these values, go to that page, and delete the real record in the page. */

        memcpy(&newRid.pageNum, pagePtr + recordOffset + 1, 4);
        memcpy(&newRid.slotNum, pagePtr + recordOffset + 5, 4);

        // Use recursion to delete the real record
        RC rc = deleteRecord(fileHandle, recordDescriptor, newRid);
    }

    /* Shifting records effectively deletes the record */
    rbfm_helpers::shiftRecordsToLeft(pagePtr, recordOffset, nextRecordOffset);

    /* Update F and N */
    /* FIXME: Update F and N */
    unsigned short F = rbfm_helpers::getRemainingBytesFromPage(pagePtr);
    //unsigned short N = rbfm_helpers::getNumberOfSlotEntriesFromPage(pagePtr);
    F = F - recordLength;
    //N = N - 1;
    memcpy(pagePtr + PAGE_SIZE - 2, &F, 2);
    //memcpy(pagePtr + PAGE_SIZE - 4, &N, 2);

    /* Set the slot directory for the deleted record to be "Dead"
     * i.e. Set the recordLength as 0 and offset to PAGE_SIZE */
    rbfm_helpers::writeDetailsToSlotNo(pagePtr, rid.slotNum, PAGE_SIZE, 0);

    /* Write the page to the file */
    RC rc = fileHandle.writePage(rid.pageNum, pagePtr);
    if(rc){
        //std::cerr << "writing page error" << std::endl;
    }

    free(pagePtr);

    return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    auto numOfFields = (unsigned) recordDescriptor.size();
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator(numOfFields);

    /* Temporary pointer to input data block pointer */
    const byte* dataPtr = (byte*)data;

    /* Data offset for actual data (without null indicator bytes) */
    int dataStartOffset = nullFieldsIndicatorActualSize;

    /* Offset for navigating data */
    int offset = 0 + nullFieldsIndicatorActualSize;

    /* Print each data field for the given record */
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    unsigned mask = 7;          // Bitwise mask, starts at 7
    for (unsigned i=0; i<numOfFields; i++) {
        const Attribute &attr = recordDescriptor[i];
        /* Check null bit */
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; mask = 7; }
        nullBit = dataPtr[nullBitIter] & 1u << mask--;

        if (nullBit) {
            std::cout << attr.name << ": NULL  ";
            continue;
        }

        /* Switch on attribute type */
        if ((attr.type == TypeInt)) {
            int value = 0;
            memcpy(&value, dataPtr + offset, sizeof(int));  // should be 4
            offset += sizeof(int);      // should be 4

            std::cout << attr.name << ": " << value << "  ";
        } else if ((attr.type == TypeReal)) {
            float value = 0;
            memcpy(&value, dataPtr + offset, sizeof(float));  // should be 4
            offset += sizeof(float);      // should be 4

            std::cout << attr.name << ": " << value << "  ";
        } else if ((attr.type == TypeVarChar)) {
            int valLen = 0;
            memcpy(&valLen, dataPtr + offset, sizeof(int));  // should be 4
            offset += sizeof(int);      // should be 4

            /* VLA are not allowed in stack (C/C++) */
            char* value = new char[valLen+1]();     // Need to include null
            memcpy(value, dataPtr + offset, valLen);
            offset += valLen;

            value[valLen] = '\0';   // Append null
            std::cout << attr.name << ": " << value << "  ";
            delete[] value;     // should be disciplined with memory management
        }
    }

    std::cout << std::endl;

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "Can't open input file" << std::endl;
        return 1;
    }

    /* Read page */
    byte *pagePtr = (byte*)malloc(PAGE_SIZE);
    int readPageResult = fileHandle.readPage(rid.pageNum, pagePtr);
    if(readPageResult != 0){
        //std::cout << "read page error" <<std::endl;
        free(pagePtr);
        return 1;
    }

    /* Get length and offset of the old record */
    unsigned short oro = 0, orl;
    rbfm_helpers::getDetailsFromSlotNo(pagePtr, rid.slotNum, &oro, &orl);
    const unsigned short OLD_REC_LEN = orl, OLD_REC_OFFSET = oro;

    /* Check if the slot is "dead" (i.e. the corresponding record is deleted ) */
    if(OLD_REC_LEN == 0){
        //std::cerr << "No record! RID(PageNum: " << rid.pageNum << ", SlotNum: " << rid.slotNum << ")" << std::endl;
        free(pagePtr);
        return 1;
    }

    /* Get length of the new record */
    const unsigned short NEW_REC_LEN = rbfm_helpers::getPredictedRecordSize(recordDescriptor, data);
    
    /* Do recursive updateRecord if record is pointer */
    RID pointedRID;
    unsigned char checkPointingRecord = 0;
    memcpy(&checkPointingRecord, pagePtr + OLD_REC_OFFSET, 1);
    
    if (checkPointingRecord == 255) {
        /* Pointer format: [11111111 (=255) (1 byte) | PageNum | slotNum ]
         * So we need to read these values, go to that page, and delete the real record in the page. */
        memcpy(&pointedRID.pageNum, pagePtr + OLD_REC_OFFSET + 1, 4);
        memcpy(&pointedRID.slotNum, pagePtr + OLD_REC_OFFSET + 5, 4);
        
        /* Recursively call updateRecord */
        RC rc = updateRecord(fileHandle, recordDescriptor, data, pointedRID);
        if (rc != 0) {      // Error handling
            //std::cerr << "Error in recursive updateRecord()!!" << std::endl;
            free(pagePtr);
            return 1;
        }
    } else {
        /* Check if new record can fit */
        if (OLD_REC_LEN == NEW_REC_LEN) {
            /* Overwrite new record to old record */
            rbfm_helpers::dataToRecord(recordDescriptor, data, pagePtr + OLD_REC_OFFSET);

            /* F, N, slot is unchanged */
        } else if (OLD_REC_LEN > NEW_REC_LEN) {     // Free space must increase
            /* Overwrite new record to old record */
            RC rc = rbfm_helpers::dataToRecord(recordDescriptor, data, pagePtr + OLD_REC_OFFSET);
            if (rc != 0) return rc;

            /* Shift all the next records to the left */
            //rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET, OLD_REC_OFFSET + NEW_REC_LEN);
            rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET + NEW_REC_LEN, OLD_REC_OFFSET + OLD_REC_LEN);


            /* Update remaining bytes to reflect changes */
            unsigned short sizeDiff = OLD_REC_LEN - NEW_REC_LEN;
            unsigned short F = rbfm_helpers::getRemainingBytesFromPage(pagePtr);
            F = F + sizeDiff;
            memcpy(pagePtr + PAGE_SIZE - 2, &F, 2);
            
            /* Update slot length */
            rbfm_helpers::writeDetailsToSlotNo(pagePtr, rid.slotNum, OLD_REC_OFFSET, NEW_REC_LEN);
        } else {            // OLD_REC_LEN < NEW_REC_LEN
            /* Potential free space = free bytes + old record length (since we will delete this) */
            const unsigned short POTENTIAL_SPACE = rbfm_helpers::getRemainingBytesFromPage(pagePtr) + OLD_REC_LEN;
            
            if (POTENTIAL_SPACE >= NEW_REC_LEN) {   // Decreases free space
                /* Shifting records effectively deletes the record */
                //rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET, OLD_REC_LEN);
                rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET, OLD_REC_OFFSET + OLD_REC_LEN);

                /* Update F to match potential free space */
                unsigned short F = rbfm_helpers::getRemainingBytesFromPage(pagePtr);
                //F = F - OLD_REC_LEN;
                F = F + OLD_REC_LEN;
                memcpy(pagePtr + PAGE_SIZE - 2, &F, 2);

                /* Calculate new record offset */
                const unsigned short N = rbfm_helpers::getNumberOfSlotEntriesFromPage(pagePtr);
                const unsigned short NEW_REC_OFFSET = PAGE_SIZE
                            - F             // Free space is different because delete
                            - 4             // F, N
                            - (4 * N);      // Number of slots

                /* Start appending updated record to start of free space */
                RC rc = rbfm_helpers::dataToRecord(recordDescriptor, data, pagePtr + NEW_REC_OFFSET);
                if (rc != 0) return rc;

                /* Update remaining bytes to reflect changes */
                //unsigned short sizeDiff = NEW_REC_LEN - OLD_REC_LEN;
                //F = F - sizeDiff;
                F = F - NEW_REC_LEN;
                memcpy(pagePtr + PAGE_SIZE - 2, &F, 2);
                
                /* Update slot's offset and length */
                rbfm_helpers::writeDetailsToSlotNo(pagePtr, rid.slotNum, NEW_REC_OFFSET, NEW_REC_LEN);
            } else { // POTENTIAL_SPACE < NEW_REC_LEN
                const unsigned short REC_PTR_SIZE = 9;  // [ptr-indicator (1 byte) | PageNum | SlotNum]

                /* Perform insertRecord */
                RID newRecRID;
                RC rc = insertRecord(fileHandle, recordDescriptor, data, newRecRID);
                if (rc != 0) return rc;

                if (REC_PTR_SIZE == OLD_REC_LEN) {
                    /* Create record pointer and overwrite */
                    rc = rbfm_helpers::memcpyRecordPointer(pagePtr + OLD_REC_OFFSET, newRecRID);
                    if (rc != 0) return rc;

                    /* No need to update F,N,slot */
                } else if (REC_PTR_SIZE < OLD_REC_LEN) {        // Increases free space
                    /* Overwrite with record pointer */
                    rc = rbfm_helpers::memcpyRecordPointer(pagePtr + OLD_REC_OFFSET, newRecRID);
                    if (rc != 0) return rc;
                    
                    /* Shift all the next records to the left */
                    //rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET, OLD_REC_OFFSET + REC_PTR_SIZE);
                    rbfm_helpers::shiftRecordsToLeft(pagePtr, OLD_REC_OFFSET + REC_PTR_SIZE, OLD_REC_OFFSET + OLD_REC_LEN);


                    /* Update remaining bytes to reflect changes */
                    unsigned short F = rbfm_helpers::getRemainingBytesFromPage(pagePtr);
                    unsigned short sizeDiff = OLD_REC_LEN - REC_PTR_SIZE;
                    F = F + sizeDiff;
                    memcpy(pagePtr + PAGE_SIZE - 2, &F, 2);

                    /* Update slot */
                    rbfm_helpers::writeDetailsToSlotNo(pagePtr, rid.slotNum, OLD_REC_OFFSET, REC_PTR_SIZE);
                } else {
                    free(pagePtr);
                    //std::cerr << "INFEASIBILITY'S REALM OF DOOM" << std::endl;
                    return 666;
                }
            }
        }
    }

    /* Write the page to the file */
    RC rc = fileHandle.writePage(rid.pageNum, pagePtr);
    if(rc){
        //std::cerr << "writing page error" << std::endl;
        return rc;
    }

    free(pagePtr);

    return 0;
}

/* FIXME: Something's wrong with readAttribute */
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    /* Return the attribute value of the record into data with format: [ oneByteNullIndicator | attribute_value ]
     * Recall that a record (called "data" before) from readRecord result has a format: [ NullBitIndicator | value_1 | ... | value_n ] */

    /* Read the record */
    byte record[PAGE_SIZE];
    RC readRecordResult = readRecord(fileHandle, recordDescriptor, rid, record);
    if(readRecordResult != 0){
        //std::cerr << "read record error" << std::endl;
    }

    int numOfFields = recordDescriptor.size();
    bool nullBit = false; int nullBitIter = 0; int mask = 7;

    int nullFieldsIndicatorActualSize = rbfm_helpers::getActualByteForNullsIndicator(numOfFields);
    byte* nullBitIndicator = new byte[nullFieldsIndicatorActualSize];
    memcpy(nullBitIndicator, record , nullFieldsIndicatorActualSize);
    unsigned short recordOffset = nullFieldsIndicatorActualSize ; //skip the null bit indicator
    unsigned char oneByteNullIndicator;

    for (unsigned i = 0 ; i < numOfFields ; i++) {
        const Attribute &attr = recordDescriptor[i];
        /* Check null bit */
        if ((i != 0) && (i % 8 == 0)) { nullBitIter++; mask = 7; }
        nullBit = nullBitIndicator[nullBitIter] & 1u << mask--;

        if(nullBit){
            /* If the field is null */
            if(attr.name == attributeName){
                /* The attribute value is null in this record. Returned data is [ 10000000 (1 byte) ] */
                oneByteNullIndicator = 1u << 7;
                memcpy( data, &oneByteNullIndicator, 1 );
                //std::cout << "This record has NULL value for " << attributeName << std::endl;
            }else{
                /* nothing to do, keep searching */
                continue;
            }
        }else{
            /* The field is not null */
            if(attr.name == attributeName){
                /* Returned data is [ 00000000 (1 byte) | attribute_value ] */
                oneByteNullIndicator = 0u;
                memcpy(data, &oneByteNullIndicator, 1);
                if( (attr.type == TypeInt) || (attr.type == TypeReal) ){ // [ int ] or [ real  ]
                    memcpy((byte*)data + 1, record + recordOffset, 4);
                }else if( attr.type == TypeVarChar ){ // [ numOfBytes | actual_value  ]
                    int numOfBytes = 0;
                    memcpy(&numOfBytes, record + recordOffset, 4);
                    memcpy((byte*)data + 1, record + recordOffset, 4);
                    recordOffset += 4;
                    memcpy((byte*)data + 5, record + recordOffset, numOfBytes);
                }
                delete[] nullBitIndicator;
                return 0;
            }
            else{
                /* If this field is not null and not the one we're looking for,
                 * increase the record offset and keep searching */
                if( (attr.type == TypeInt) || (attr.type == TypeReal) ){
                    recordOffset += 4;
                }else if( attr.type == TypeVarChar ){
                    int numOfBytes = 0;
                    memcpy(&numOfBytes, record + recordOffset, 4);
                    recordOffset += 4 + numOfBytes;
                }
            }
        } // end the case of not null
    } // end of for loop

    //std::cerr << "Cannot find the attribute in this record" << std::endl;
    delete[] nullBitIndicator;
    return 1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    /* Check if fileHandle is associated with a file */
    if(fileHandle.filePointer == nullptr){
        //std::cerr << "FileHandle is not associated with a file!!" << std::endl;
        return 1;
    }

    /* Copy file */
    std::chrono::milliseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::system_clock::now().time_since_epoch()
    );
    std::string tempFileName = std::to_string(ms.count()) + fileHandle.thisFileName;
    RC rc = pfm_helpers::copyFile(fileHandle, tempFileName);

    /* Use temp file as the RBFM_ScanIterator's fileHandle */
    FileHandle tempFile;
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    rc = rbfm.openFile(tempFileName, tempFile);
    if (rc != 0) {
        //std::cerr << "Error in openFile at RBFM::Scan()" << std::endl;
        return 1;
    }

    /* Setup ScanIterator */
    return rbfm_ScanIterator.initScanIterator(tempFile,
            recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

//////////////// RBFM_ScanIterator Definitions //////////////////

RBFM_ScanIterator::RBFM_ScanIterator() {
    compOp = NO_OP;
    value = nullptr;

    isInitialized = false;
    RIDCursor.slotNum = 0;
    RIDCursor.pageNum = 0;
    RIDcachedData.slotNum = 0;
    RIDcachedData.pageNum = 0;
    cachedPageNum = 0;
    cachedPageNumOfSlots = 0;
    cachedPage = nullptr;
    cachedData = nullptr;
    fileNumOfPages = 0;
    isEOF = false;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {}

RC RBFM_ScanIterator::initScanIterator(const FileHandle fh, const std::vector<Attribute> &rd,
                                      const std::string &ca, const CompOp co, const void *v,
                                      const std::vector<std::string> &an) {
    /* Error checking */
    if (isInitialized) {
        //std::cerr << "ScanIterator is already initialized!" << std::endl;
        return 1;
    }

    /* Set scan variables */
    fileHandle = fh;
    recordDescriptor = rd;
    conditionAttribute = ca;
    compOp = co;
    value = v;
    attributeNames = an;

    isInitialized = true;
    RIDCursor.slotNum = 0;
    RIDCursor.pageNum = 0;
    cachedPage = new byte[PAGE_SIZE];
    cachedData = new byte[PAGE_SIZE];        // Record cannot exceed PAGE_SIZE
    fileNumOfPages = fileHandle.getNumberOfPages();

    /* Cache first page if file is not empty */
    if (fileNumOfPages > 0) {
        cachedPageNum = RIDCursor.pageNum;
        RC rc = fileHandle.readPage(cachedPageNum, cachedPage);
        if (rc != 0) {
            //std::cerr << "Error in readPage()" << std::endl;
            return 1;
        }
        cachedPageNumOfSlots = rbfm_helpers::getNumberOfSlotEntriesFromPage(cachedPage);

        /* Find the first record to match the filter */
        rc = findAndCacheRecord();
        if (rc != 0) {
            //std::cerr << "Error in findAndCacheNextRecord()" << std::endl;
            return 1;
        }
    } else {
        /* Set EOF to scanIterator */
        isEOF = true;
    }

    return 0;
}

/* Finds and caches the record currently pointed by ridCursor, caches the page if needed.
 * Increase the ridCursor after cache operation.
 * Sets EOF if current ridCursor points to the end of file.
 * Member variables it touches:
 *      curPage, curRecord, isEOF, cachedPageNum, ridCursor */
RC RBFM_ScanIterator::findAndCacheRecord() {
    /* Error checking */
    if (!isInitialized) {
        //std::cerr << "Initialize Scan Iterator first!" << std::endl;
        return 1;
    }

    /* Keep caching record until found */
    bool found = false;
    while (!found) {
        /* EOF check */
        if (RIDCursor.pageNum >= fileNumOfPages) {
            isEOF = true;
            return 0;
        }

        /* Cache new page if different */
        if (cachedPageNum != RIDCursor.pageNum) {
            cachedPageNum = RIDCursor.pageNum;
            RC rc = fileHandle.readPage(cachedPageNum, cachedPage);
            if (rc != 0) {
                //std::cerr << "Error in readPage()" << std::endl;
                return 1;
            }
            cachedPageNumOfSlots = rbfm_helpers::getNumberOfSlotEntriesFromPage(cachedPage);
        }

        /* Check if rid is dead */
        unsigned int nSlotOffset = PAGE_SIZE - ( 4 + 4 * (RIDCursor.slotNum+1) );
        unsigned short deadChecker = 0;
        memcpy(&deadChecker, cachedPage + nSlotOffset + 2, 2);
        if(deadChecker == 0) {
            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }

            continue;
        }

        /* read record from cached page */
        RC rc = rbfm_helpers::readRecordFromPage(fileHandle, recordDescriptor,
                RIDCursor, cachedPage, cachedData);
        if (rc != 0) {
            //std::cerr << "Error in readRecordFromPage!" << std::endl;
            return rc;
        }

        /* Perform delete record for the cached page (eliminate duplicate records) */
        rc = rbfm_helpers::deleteRecordFromPage(fileHandle, recordDescriptor,
                                                RIDCursor, cachedPage);
        if (rc != 0) {
            //std::cerr << "Error in deleteRecordFromPage!" << std::endl;
            return rc;
        }

        /* Stop and advance RID if NO_OP */
        if (compOp == NO_OP) {
            found = true;
            RIDcachedData = RIDCursor;

            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }
            continue;
        }

        /* Buffer for record representation for the return data */
        byte recordRepBuffer[PAGE_SIZE];
        //int recordRepSize = 0;
        rc = rbfm_helpers::dataToRecord(recordDescriptor, cachedData, recordRepBuffer);

        /* Get the position of the condition attribute */
        unsigned conditionAttrPos = 0;
        Attribute actualConditionAttr;
        for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
            if (conditionAttribute == recordDescriptor.at(i).name) {
                conditionAttrPos = i;
                actualConditionAttr = recordDescriptor.at(i);
                break;
            }
        }

        /* Get null bit for condition value */
        int nullBitIter = 0;        // Iterator for byte-null-indicator
        unsigned actualPos = conditionAttrPos % 8;
        unsigned nullIndicatorSize = rbfm_helpers::getActualByteForNullsIndicator(recordDescriptor.size());
        for (unsigned j = 0; j < nullIndicatorSize; ++j) {
            if ((j != 0) && (j % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read
        }

        /* Go to next record if conditionAttribute's value is NULL */
        bool nullBit = cachedData[nullBitIter] & (1u << (7u - actualPos));   // 7u - actualPos
        if (nullBit) {
            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }
            found = false;
            continue;
        }

        /* Calculate offset of the operand value */
        unsigned tempOffset = 0;
        if (conditionAttrPos == 0) {
            tempOffset = 4 + nullIndicatorSize + (4 * recordDescriptor.size());
        } else {
            unsigned offset = 4 + nullIndicatorSize + (4 * (conditionAttrPos - 1));
            memcpy(&tempOffset, recordRepBuffer + offset, 4);
        }

        /* Setup conditional attribute value buffer */
        unsigned conditionAttrOperandLen;
        byte* conditionAttrOperand = nullptr;
        unsigned plzWorkIBegYou = 0;        // Offset for writing to operand value
        if (actualConditionAttr.type == TypeVarChar) {
            /* Get length of actual string */
            unsigned endo = 4 + nullIndicatorSize + (4 * (conditionAttrPos));
            unsigned end;
            memcpy(&end, recordRepBuffer + endo, 4);
            int size = (int)end - (int)tempOffset;

            conditionAttrOperandLen = size;
            conditionAttrOperand = new byte[conditionAttrOperandLen + 4];       // varchar: [len | string]
            memcpy(conditionAttrOperand + plzWorkIBegYou, &size, 4);        // add length to first 4 bytes
            plzWorkIBegYou += 4;
        } else {
            conditionAttrOperandLen = actualConditionAttr.length;      // Should be 4
            conditionAttrOperand = new byte[conditionAttrOperandLen];
        }

        /* Copy actual condition attribute value */
        memcpy(conditionAttrOperand + plzWorkIBegYou, recordRepBuffer + tempOffset, conditionAttrOperandLen);

        if (actualConditionAttr.type == TypeInt) {
            /* Convert to int */
            int operand = 0;
            memcpy(&operand, conditionAttrOperand, 4);
            //const int* reference = static_cast<const int*>(value);
            int refValue = 0;
            memcpy(&refValue, value, 4);

            /* Switch on comparison operator */
            if (compOp == EQ_OP) {
                if (operand == refValue) found = true;
            } else if (compOp == LT_OP) {
                if (operand < refValue) found = true;
            } else if (compOp == LE_OP) {
                if (operand <= refValue) found = true;
            } else if (compOp == GT_OP) {
                if (operand > refValue) found = true;
            } else if (compOp == GE_OP) {
                if (operand >= refValue) found = true;
            } else if (compOp == NE_OP) {
                if (operand != refValue) found = true;
            } else {
                //std::cerr << "UNDEFINED OPERATOR in findAndCacheNextRecord()" << std::endl;
                return 1;
            }

            if (found) RIDcachedData = RIDCursor;

            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }
        } else if (actualConditionAttr.type == TypeReal) {
            /* Convert to int */
            float operand = 0;
            memcpy(&operand, conditionAttrOperand, 4);
            //const int* reference = static_cast<const int*>(value);
            float refValue = 0;
            memcpy(&refValue, value, 4);

            /* Switch on comparison operator */
            if (compOp == EQ_OP) {
                if (operand == refValue) found = true;
            } else if (compOp == LT_OP) {
                if (operand < refValue) found = true;
            } else if (compOp == LE_OP) {
                if (operand <= refValue) found = true;
            } else if (compOp == GT_OP) {
                if (operand > refValue) found = true;
            } else if (compOp == GE_OP) {
                if (operand >= refValue) found = true;
            } else if (compOp == NE_OP) {
                if (operand != refValue) found = true;
            } else {
                //std::cerr << "UNDEFINED OPERATOR in findAndCacheNextRecord()" << std::endl;
                return 1;
            }

            if (found) RIDcachedData = RIDCursor;

            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }
        } else if (actualConditionAttr.type == TypeVarChar) {
            /* Convert operand to c str */
            unsigned please = 0;
            int valLen = 0;
            memcpy(&valLen, conditionAttrOperand + please, sizeof(int));  // should be 4
            please += sizeof(int);      // should be 4

            char* operand = new char[valLen+1]();     // Need to include null
            memcpy(operand, conditionAttrOperand + please, valLen);
            operand[valLen] = '\0';   // Append null

            /* Convert internal value to c str */
            please = 0;
            valLen = 0;
            memcpy(&valLen, (byte*)value + please, sizeof(int));  // should be 4
            please += sizeof(int);      // should be 4

            char* refValue = new char[valLen+1]();     // Need to include null
            memcpy(refValue, (byte*)value + please, valLen);
            refValue[valLen] = '\0';   // Append null

            /* Switch on comparison operator */
            if (compOp == EQ_OP) {
                if (strcmp(operand, refValue) == 0) found = true;
            } else if (compOp == LT_OP) {
                if (strcmp(operand, refValue) < 0) found = true;
            } else if (compOp == LE_OP) {
                if (strcmp(operand, refValue) <= 0) found = true;
            } else if (compOp == GT_OP) {
                if (strcmp(operand, refValue) > 0) found = true;
            } else if (compOp == GE_OP) {
                if (strcmp(operand, refValue) >= 0) found = true;
            } else if (compOp == NE_OP) {
                if (strcmp(operand, refValue) != 0) found = true;
            } else {
                //std::cerr << "UNDEFINED OPERATOR in findAndCacheNextRecord()" << std::endl;
                delete[] operand;
                delete[] refValue;
                return 1;
            }
            delete[] operand;
            delete[] refValue;
            if (found) RIDcachedData = RIDCursor;

            /* Advance to next rid */
            if (cachedPageNumOfSlots == 0) { // Move to next page
                RIDCursor.slotNum = 0;
                RIDCursor.pageNum += 1;
            } else {
                if (RIDCursor.slotNum < cachedPageNumOfSlots - 1) {
                    RIDCursor.slotNum += 1;
                } else {
                    RIDCursor.slotNum = 0;
                    RIDCursor.pageNum += 1;     // Advance to next page if we iterated all records in the current page
                }
            }
        }

        delete[] conditionAttrOperand;
    }

    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    if (isEOF) {
        return RBFM_EOF;
    };

    /* Only memcpy the relevant values specified by attribute */
    /* First, get the positions of the attributeNames */
    std::unordered_map<std::string,unsigned> attrPosMap;
    for (const std::string &aname: attributeNames) {
        for (unsigned i = 0; i < recordDescriptor.size(); ++i) {
            if (aname == recordDescriptor.at(i).name) {
                attrPosMap[aname] = i;
            }
        }
    }

    /* Temporary buffer for record-representation of the data */
    byte recordRep[PAGE_SIZE];// int recordRepSize = 0;
    RC rc = rbfm_helpers::dataToRecord(recordDescriptor,
            cachedData, recordRep);
    if (rc != 0) {
        //std::cerr << "Error in getNextRecord() dataToRecord()" << std::endl;
        return rc;
    }

    /* Construct null-byte indicator */
    byte* dataPtr = (byte*)data;
    unsigned numOfAFields = attributeNames.size();          // Actual number of fields
    unsigned nullSizeForReturnData = rbfm_helpers::getActualByteForNullsIndicator((int)numOfAFields);

    memset(dataPtr, 0, nullSizeForReturnData);     // Set to all zeroes for null-byte indicator

    /* Copy data while setting null bits */
    int nullBitIter = 0;        // Iterator for byte-null-indicator
    const byte MASK = 128;            // Bit mask
    unsigned writeOffset = nullSizeForReturnData;
    for (unsigned j = 0; j < numOfAFields; ++j) {
        if ((j != 0) && (j % 8 == 0)) { nullBitIter++; }  // Advance null byte iterator if 8 bits are read

        /* Construct null-bit indicator
         * TODO: Potential bug might be found here */
        unsigned attributePos = attrPosMap[attributeNames[j]];
        const byte i_result = MASK & (byte)(cachedData[nullBitIter] << (attributePos % 8));
        dataPtr[nullBitIter] = dataPtr[nullBitIter] | (byte)(i_result >> (j % 8));

        /* memcpy record if not null */
        if (i_result != MASK) {
            /* Get offsets */
            unsigned nullSizeForCacheData = rbfm_helpers::getActualByteForNullsIndicator(recordDescriptor.size());
            unsigned offsetFieldStartForCacheData = 4 + nullSizeForCacheData;
            unsigned endOffset = 0, startOffset = 0;
            if (attributePos == 0) {
                startOffset = 4 + nullSizeForCacheData + (4 * recordDescriptor.size());
            } else {
                memcpy(&startOffset, recordRep + offsetFieldStartForCacheData + (4 * (attributePos - 1)), 4);
            }
            memcpy(&endOffset, recordRep + offsetFieldStartForCacheData + (4 * attributePos), 4);

            /* Memcpy contents to data */
            if (recordDescriptor[attributePos].type == TypeVarChar) {
                int valueLen = (int)endOffset - (int)startOffset;
                memcpy(dataPtr + writeOffset, &valueLen, 4);        // Write length first
                writeOffset += 4;
                memcpy(dataPtr + writeOffset, recordRep + startOffset, valueLen);
                writeOffset += valueLen;
            } else {
                unsigned valueLen = endOffset - startOffset;
                memcpy(dataPtr + writeOffset, recordRep + startOffset, valueLen);
                writeOffset += valueLen;
            }
        }
    }

    rid = RIDcachedData;

    /* Call find next record -> sets the internal cached record to next record */
    rc = findAndCacheRecord();
    if (rc != 0) {
        //std::cerr << "Error in findAndCacheRecord() in getNextRecord()" << std::endl;
        return rc;
    }

    return 0;
}

RC RBFM_ScanIterator::close() {
    delete[] cachedPage;
    delete[] cachedData;

    // TODO: Close and destroy temp file
    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
    std::string tempFile = fileHandle.thisFileName;
    rbfm.closeFile(fileHandle);
    rbfm.destroyFile(tempFile);

    compOp = NO_OP;
    value = nullptr;

    isInitialized = false;
    RIDCursor.slotNum = 0;
    RIDCursor.pageNum = 0;
    RIDcachedData.slotNum = 0;
    RIDcachedData.pageNum = 0;
    cachedPageNum = 0;
    cachedPageNumOfSlots = 0;
    cachedPage = nullptr;
    cachedData = nullptr;
    fileNumOfPages = 0;
    isEOF = false;

    return 0;
}

//////////// RID Definitions ////////////

bool _rid::operator==(const _rid &rhs) const {
    return pageNum == rhs.pageNum &&
           slotNum == rhs.slotNum;
}

bool _rid::operator!=(const _rid &rhs) const {
    return !(rhs == *this);
}

bool _rid::operator<(const _rid &rhs) const {
    if (pageNum < rhs.pageNum)
        return true;
    if (rhs.pageNum < pageNum)
        return false;
    return slotNum < rhs.slotNum;
}

bool _rid::operator>(const _rid &rhs) const {
    return rhs < *this;
}

bool _rid::operator<=(const _rid &rhs) const {
    return !(rhs < *this);
}

bool _rid::operator>=(const _rid &rhs) const {
    return !(*this < rhs);
}
