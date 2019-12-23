#ifndef _pfm_h_
#define _pfm_h_


typedef unsigned PageNum;
/**
 * Return Code
 * 0: normal completion
 * 1: errors from PagedFileManager */
typedef int RC;
typedef unsigned char byte;


#define PAGE_SIZE 4096

#include <string>
#include <climits>
#include <cstdio>
#include <sys/stat.h>
#include <iostream>
#include <cmath>

class FileHandle;

/**
 * Namespace for using helper functions */
namespace pfm_helpers {
    /**
     * Converts unsigned int to char array (big-endian).
     * Writes directly to the given byte array.
     * Warning: does NOT check for buffer length.. Use with caution.
     *
     * @param byteArr the array to be written to. MUST be at least 4 + start bytes
     * @param start starting index */
    void unsignedIntToBytes(unsigned inputInt, byte* byteArr, unsigned start);

    /**
     * Converts unsigned int to char array (big-endian)
     * Warning: does NOT check for buffer length.. Use with caution.
     *
     * @param byteArr the array to be read from. MUST be at least 4 + start bytes
     * @param start starting index */
    unsigned bytesToUnsignedInt(const byte* byteArr, unsigned start);

    /**
     * From test_utils.h
     * Checks whether the file exists */
    bool fileExists(const std::string &fileName);

    /**
     * Update counter values of fileHandle into the file pointed by fileHandle
     * @param fileHandle
     * @return 0 with no error, 1 with error
     */
    int UpdateCounters(FileHandle &fileHandle);

    /**
     * Copy file associated with srcFileHandle with name as destFileName
     * @param srcFileHandle
     * @param destFileName
     * @return
     */
    RC copyFile(const FileHandle &srcFileHandle, const std::string &destFileName);
}

/**
 * The PagedFileManager class handles the creation, deletion, opening, and closing of paged files.
 * Warning: should maintain only one instance of this class, and all requests for PF component file management
 *  should be directed to that instance.
 */
class PagedFileManager {
public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance

    /**
     * Create an empty-paged file called fileName.
     * The file should not already exist.
     * This method should NOT create any pages in the file.
     *
     * @param fileName name of the file to be created
     * @return RC return code */
    RC createFile(const std::string &fileName);                         // Create a new file

    /**
     * Destroy the paged file whose name is fileName.
     * The file should already exist.
     *
     * @param fileName name of the file */
    RC destroyFile(const std::string &fileName);                        // Destroy a file

    /** 
     * Opens the paged file whose name is fileName.
     * The file must already exist (and been created using the createFile method).
     * If successful, the fileHandle object whose address is passed in as a parameter
     * now becomes a "handle" for the open file.
     * It is an error if fileHandle is already a handle for some open file.
     * It is not an error to open the same file more than once if desired,
     * but this would be done by using a different fileHandle object each time.
     * Each call to the openFile method creates a new "instance" of FileHandle.
     * Warning: No need to prevent multiple openings of a file
     *
     * @param fileName name of the file
     * @param fileHandle the handle of the opened file
     * @return RC return code */
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file

    /** 
     * Closes the open file instance referred to by fileHandle.
     * All of the file's pages are flushed to disk when the file is closed.
     *
     * @param fileHandle the filehandle of the file */
    RC closeFile(FileHandle &fileHandle);                               // Close a file

protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

private:
    static PagedFileManager *_pf_manager;
};

/**
 * The FileHandle class provides access to the pages of an open file.
 * To access the pages of a file, a client first creates an instance of this class
 * and passes it to the PagedFileManager::openFile method. */
class FileHandle {
public:
    /**
     * Counters
     * 1. Needs to be persistent with multiple file openings.
     * 2. Need to be stored physically in a file.
     * 3. No need to worry about concurrency. */
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    FILE * filePointer;
    unsigned tableIdCounter;
    unsigned schemaVersioningCounter;
    std::string thisFileName;

    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor

    /**
     *  This method reads the page into the memory block pointed to by data.
     *  The page should exist. Note that page numbers start from 0.
     *
     *  @param pageNum page number to be read from.
     *  @param data pointer to the block of data that should be written to.
     *  @return RC return code */
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page

    /**
     * This method writes the given data into a page specified by pageNum.
     * The page SHOULD exist. Page numbers start from 0.
     *
     * @param pageNum page to be written to.
     * @param data block of data that should be read from.
     * @return RC return code */
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page

    /**
     * This method appends a new page to the end of the file
     * and writes the given data into the newly allocated page.
     *
     * @param data the pointer to the input data
     * @return RC return code */
    RC appendPage(const void *data);                                    // Append a specific page

    /**
     * This method returns the total number of pages currently in the file.
     *
     * @return unsigned number of pages in the file. */
    unsigned getNumberOfPages();                                        // Get the number of pages in the file

    /**
     * Returns the current counter values of this FileHandle in the three given variables.
     *
     * @param readPageCount reference to the output
     * @param writePageCount reference to the output
     * @param appendPageCount reference to the output
     * @return RC return code. */
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables
};

#endif