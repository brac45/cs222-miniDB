#include <fstream>
#include "pfm.h"
#include <cstdio>
#include "iostream"

//////////// pfm_helpers Definitions ////////////

void pfm_helpers::unsignedIntToBytes(unsigned inputInt, byte *byteArr, unsigned start) {
    byteArr[start] = inputInt >> 24u & 0xFFu;
    byteArr[start+1] = inputInt >> 16u & 0xFFu;
    byteArr[start+2] = inputInt >> 8u & 0xFFu;
    byteArr[start+3] = inputInt & 0xFFu;
}

unsigned pfm_helpers::bytesToUnsignedInt(const byte *byteArr, unsigned start) {
    unsigned retVal = 0;
    retVal = (retVal << 8u) + byteArr[start];
    retVal = (retVal << 8u) + byteArr[start+1];
    retVal = (retVal << 8u) + byteArr[start+2];
    retVal = (retVal << 8u) + byteArr[start+3];

    return retVal;
}

bool pfm_helpers::fileExists(const std::string &fileName) {
    struct stat stFileInfo{};
    return stat(fileName.c_str(), &stFileInfo) == 0;
}

int pfm_helpers::UpdateCounters(FileHandle &fileHandle){
    byte buffer[PAGE_SIZE] = { 0 };
    pfm_helpers::unsignedIntToBytes(fileHandle.readPageCounter, buffer, 0);
    pfm_helpers::unsignedIntToBytes(fileHandle.writePageCounter, buffer, 4);
    pfm_helpers::unsignedIntToBytes(fileHandle.appendPageCounter, buffer, 8);

    int seekResult = fseek(fileHandle.filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error while update" << std::endl;
        return 1;
    }

    auto updateCounter = fwrite(buffer, sizeof(byte), PAGE_SIZE, fileHandle.filePointer);
    if (updateCounter != PAGE_SIZE) {
        //std::cerr << "fwrite error while update" << std::endl;
        return 1;
    }
    return 0;
}

RC pfm_helpers::copyFile(const FileHandle &srcFileHandle, const std::string &destFileName) {
    /* Check if fileHandle is already associated with a file */
    if (srcFileHandle.filePointer == nullptr) {
        //std::cerr << "FileHandle is not associated with a file!!" << std::endl;
        return 1;
    }

    /* Get new file pointer for the FileHandle object */
    FILE * srcfptr = fopen(srcFileHandle.thisFileName.c_str(), "rb");
    if (srcfptr == nullptr) {
        //std::cerr << "Unable to source file" << std::endl;
        return 1;
    }

    FILE * destfptr = fopen(destFileName.c_str(), "wb");
    if (destfptr == nullptr) {
        //std::cerr << "Unable to destination file" << std::endl;
        return 1;
    }

    /* Copy file to dest */
    size_t readBytes;
    byte buff[PAGE_SIZE];
    while (0 < (readBytes = fread(buff, 1, sizeof(buff), srcfptr)))
        fwrite(buff, 1, readBytes, destfptr);

    /* Close files */
    fclose(srcfptr);
    fclose(destfptr);

    return 0;
}

//////////// PagedFileManager Definitions ////////////

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;


RC PagedFileManager::createFile(const std::string &fileName) {
    //std::cout<<"I'm in createFile"<<std::endl;

    /* Check for file */
    if (pfm_helpers::fileExists(fileName)) {
        //std::cerr << "File already exists!" << std::endl;
        return 1;
    }


    /* Create the file in storage */
    FILE* fptr = nullptr;
    fptr = fopen(fileName.c_str(), "wb");
    if (fptr == nullptr) {
        //std::cerr << "Unable to open file!" << std::endl;
        return 1;
    }

    /* Write hidden page
     * Has meta-data in this order:
     * 0-3. readPageCounter
     * 4-7. writePageCounter
     * 8-11. appendPageCounter */
    byte buffer[PAGE_SIZE] = { 0 };
    auto writtenBytes = fwrite(buffer, sizeof(byte), PAGE_SIZE, fptr);
    if (writtenBytes != PAGE_SIZE) {
        //std::cerr << "fwrite error" << std::endl;
        return 1;
    }

    fclose(fptr);

    //std::cout<<"I'm out createFile"<<std::endl;

    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {

    //std::cout<<"I'm in destroyFile"<<std::endl;

    /* Check for file */
    if (!pfm_helpers::fileExists(fileName)) {
        //std::cerr << "File does not exist!" << std::endl;
        return 1;
    }

    /* Destroy the file in the storage */
    if (remove(fileName.c_str()) != 0) {
        //std::cerr << "Unable to delete file!" << std::endl;
        return 1;
    } else { //std::cout<<"I'm out destroyFile"<<std::endl;
        return 0; }
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

    //std::cout<<"I'm in openFile"<<std::endl;

    /* Check if fileHandle is already associated with a file */
    if (fileHandle.filePointer != nullptr) {
        //std::cerr << "fileHandle is already associated with a file!" << std::endl;
        return 1;
    }

    /* Try to open file */
    FILE * fptr = fopen(fileName.c_str(), "rb+");
    if (fptr == nullptr) {
        //std::cerr << "Unable to open file" << std::endl;
        return 1;
    }

    /* Set filename string */
    fileHandle.thisFileName = fileName;

    /* Load hidden page to memory */
    byte buffer[PAGE_SIZE] = { 0 };
    auto readBytes = fread(buffer, sizeof(byte), PAGE_SIZE, fptr);
    if (readBytes != PAGE_SIZE) {
        //std::cerr << "fread error" << std::endl;
        return 1;
    }

    /* Set fileHandle members */
    fileHandle.readPageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 0);
    fileHandle.writePageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 4);
    fileHandle.appendPageCounter = pfm_helpers::bytesToUnsignedInt(buffer, 8);
    fileHandle.filePointer = fptr;          // Warning: filePointer now points to offset after the hidden page
    fileHandle.tableIdCounter = pfm_helpers::bytesToUnsignedInt(buffer, 12);


    //std::cout<<"I'm out openFile"<<std::endl;

    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {

    //std::cout<<"I'm in closeFile"<<std::endl;

    /* Check if fileHandle is associated with a file */
    if (fileHandle.filePointer == nullptr) {
        //std::cerr << "fileHandle is not associated with a file!" << std::endl;
        return 1;
    }

    /* Write counters to hidden page */
    byte buffer[PAGE_SIZE] = { 0 };
    pfm_helpers::unsignedIntToBytes(fileHandle.readPageCounter, buffer, 0);
    pfm_helpers::unsignedIntToBytes(fileHandle.writePageCounter, buffer, 4);
    pfm_helpers::unsignedIntToBytes(fileHandle.appendPageCounter, buffer, 8);
    pfm_helpers::unsignedIntToBytes(fileHandle.tableIdCounter, buffer, 12);

    /* Before closing file, need to save counter values
     * For that, move filePointer to the beginning of the file, */
    int seekResult = fseek(fileHandle.filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }

    /* and write them in the first hidden page */
    auto writtenBytes = fwrite(buffer, sizeof(byte), PAGE_SIZE, fileHandle.filePointer);
    if (writtenBytes != PAGE_SIZE) {
        //std::cerr << "fwrite error" << std::endl;
        return 1;
    }

    /* Close filePointer */
    fclose(fileHandle.filePointer);
    fileHandle.filePointer = nullptr;

    //std::cout<<"I'm out closeFile"<<std::endl;

    return 0;
}

//////////////////// FileHandle Definitions /////////////////////////

FileHandle::FileHandle() {

    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    filePointer = nullptr;
    tableIdCounter = 0;
    schemaVersioningCounter = 0;
    thisFileName = "";
}

FileHandle::~FileHandle() = default;



RC FileHandle::readPage(PageNum pageNum, void *data) {

    //std::cout<<"I'm in readPage"<<std::endl;

    /* Page existence check */
    if(pageNum >= this->appendPageCounter){
        //std::cerr << "page does not exist" << std::endl;
        return 1;
    }

    /* Reposition the filePointer
     * As the first page is for hidden page, need to go (PageNum + 1)-th page */
    int seekResult = fseek(this->filePointer, (pageNum+1)*PAGE_SIZE, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }

    /* Copy one page from the file (pointed by filePointer) to the data */
    auto readBytes = fread(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (readBytes != PAGE_SIZE) {
        //std::cerr << "fread error" << std::endl;
        return 1;
    }

    /* Increase the readPageCounter value */
    this->readPageCounter = this->readPageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = pfm_helpers::UpdateCounters(*this);
    if(updateResult != 0){
        //std::cerr << "update error" <<std::endl;
        return 2;
    }

    //std::cout<<"I'm out readPage"<<std::endl;

    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {

    //std::cout<<"I'm in writePage"<<std::endl;

    /* Page existence check */
    if(pageNum >= this->appendPageCounter){
        //std::cerr << "page does not exist" << std::endl;
        return 1;
    }

    /* Reposition the filePointer
     * As the first page is for hidden page, need to write in (PageNum + 1)-th page */
    int seekResult = fseek(this->filePointer, (pageNum+1)*PAGE_SIZE, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }

    /* Write the data into one page in the file (pointed by filePointer)
     * As the first page is for hidden page, need to write in (PageNum + 1)-th page */
    auto writtenBytes = fwrite(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (writtenBytes != PAGE_SIZE) {
        //std::cerr << "fwrite error" << std::endl;
        return 1;
    }

    /* Increase the writePageCounter value */
    this->writePageCounter = this->writePageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = pfm_helpers::UpdateCounters(*this);
    if(updateResult != 0){
        //std::cerr << "update error" <<std::endl;
        return 2;
    }

    //std::cout<<"I'm out writePage"<<std::endl;
    return 0;
}

RC FileHandle::appendPage(const void *data) {

    //std::cout<<"I'm in appendPage"<<std::endl;

    /* Reposition filePointer to the end of the file */
    int seekResult = fseek(this->filePointer, 0, SEEK_END);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }

    /* Append the data into the last page of the file */
    auto writtenBytes = fwrite(data, sizeof(byte), PAGE_SIZE, this->filePointer);
    if (writtenBytes != PAGE_SIZE) {
        //std::cerr << "fwrite error" << std::endl;
        return 1;
    }

    /* Increase the appendPageCounter value */
    this->appendPageCounter = this->appendPageCounter + 1;

    /* Update the counter value in file's hidden page */
    int updateResult = pfm_helpers::UpdateCounters(*this);
    if(updateResult != 0){
        //std::cerr << "update error" <<std::endl;
        return 2;
    }

    return 0;
}

unsigned FileHandle::getNumberOfPages() {

    //std::cout<<"I'm in getNumberOfPage"<<std::endl;

    long start, end;

    /* Seek the beginning of the file and save the position into start */
    int seekResult = fseek(this->filePointer, 0, SEEK_SET);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }
    start = ftell(this->filePointer);

    /* Seek the end of the file and save the position into end */
    seekResult = fseek(this->filePointer, 0, SEEK_END);
    if (seekResult != 0){
        //std::cerr << "fseek error" << std::endl;
        return 1;
    }
    end = ftell(this->filePointer);

    /* Calculate the number of pages and substitute 1 for hidden page */
    //std::cout<<"start is "<<start<<" end  is "<<end<<"return value is "<< (end - start)/PAGE_SIZE - 1<<std::endl;
    //std::cout<<"I'm out getNumberOfPage"<<std::endl;
    return (end - start)/PAGE_SIZE - 1;

}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {

    //std::cout<<"I'm in collectCounterValues"<<std::endl;

    /* Save current counter values of this FileHandle into given variables */
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;

    //std::cout<<"I'm out collectCounterValues"<<std::endl;
    return 0;

}






