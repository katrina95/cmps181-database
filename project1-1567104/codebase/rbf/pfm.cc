#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>>

#include "pfm.h"

PagedFileManager *PagedFileManager::_pf_manager = 0;

PagedFileManager *PagedFileManager::instance() {
    if (!_pf_manager) {
        _pf_manager = new PagedFileManager();
    }
    return _pf_manager;
}


PagedFileManager::PagedFileManager() {
}


PagedFileManager::~PagedFileManager() {
}


RC PagedFileManager::createFile(const string &fileName) {
    try {
        if (fileExists(fileName) == true) {
            cout << "File exists!" << endl;
            return -1;
        }
        //FILE * fopen ( const char * filename, const char * mode );
        //Opens the file, return a "stream" associated with it (identified by FILE pointer)
        //r: read    w:write   +:update(both for input and output)
        FILE *file = fopen(fileName.c_str(), "wb");  //.c_str() returns "C string"
        //int fclose ( FILE * stream );
        //Closes the file associated with the stream and disassociates it.
        fclose(file);
        cout << "File created!" << endl;
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}




RC PagedFileManager::destroyFile(const string &fileName) {
    try {
        if (fileExists(fileName) == false) {
            cout << "File doesn't exist!" << endl;
            return -1;
        }
        //No streams are involved in the operation.
        //Deletes the file
        if(remove(fileName.c_str()) == 0) {
            return 0;
        }else{
            cout << "File can not be deleted!" << endl;
            return -1;
        }
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    try {
        if (!fileExists(fileName)) {
            cout << "File doesn't exist!" << endl;
            return -1;
        }
        FILE *currentFileHandled = fopen(fileName.c_str(), "rb+");
        //Same type: FILE pointer
        //FILE *currentFileHandled;
        fileHandle.currentFileHandled = currentFileHandled;
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

//Close the FileHandle's current handled file
RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    try {
        //int fflush ( FILE * stream );
        //If the given stream was open for writing(w) (or if it was open for updating(+) and the last i/o operation was an output operation)
        //any unwritten data in its "output buffer" is written to the file.
        if(fflush(fileHandle.currentFileHandled) != 0){
            cout << "File can't be flushed!" << endl;
            return -1;
        }
        if(fclose(fileHandle.currentFileHandled) != 0){
            cout << "File can't be closed!" << endl;
            return -1;
        }
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

bool PagedFileManager::fileExists(string fileName) {
    struct stat buffer;
    //The fastest way to check file existence. C POSIX Library, Unix system call.
    //Argument: (const char *filename, struct stat *buf);
    //Get stat struct of filename, then put into buffer
    //On success return 0, on error return -1
    return (stat(fileName.c_str(), &buffer) == 0);
}

//Constructor
FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() {
}

/**
 * Read page into the memory block pointed to by "data"
 * The page should exist
 * Page numbers start from 0
 *
 * @param pageNum
 * @param data
 * @return
 */
RC FileHandle::readPage(PageNum pageNum, void *data) {
    try {
        if (currentFileHandled == NULL) {
            cout << "Current file handled is NULL!" << endl;
            return -1;
        }
        if ((pageNum < 0) || (pageNum >= getNumberOfPages())) {
            cout << "Illegal page number!" << endl;
            return -1;
        }
        //int fseek ( FILE * stream, long int offsetToStartOfFreespace, int origin );
        //Only sets the position indicator associated with the stream to a new position.
        //For streams open in binary mode, the new position = offsetToStartOfFreespace + origin
        if (fseek(currentFileHandled, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
            cout << "Illegal position indicator!" << endl;
            return -1;
        }
        //SEEK_CUR of currentFileHandled is changed after above "fseek()"
        //size_t fread ( void * ptr, size_t size, size_t count, FILE * stream );
        //Read N blocks of data from stream
        //Reads an array of count elements, each one with a size of size bytes, from the stream and stores them in the block of memory specified by ptr.
        if(fread(data, PAGE_SIZE, 1, currentFileHandled) != 1){
            cout << "Reading error or EOF!" << endl;
            return -1;
        }
        readPageCounter++;
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

/**
 * This method writes the given data into a page specified by pageNum.
 * Page numbers start from 0.
 *
 * @param pageNum
 * @param data
 * @return
 */
RC FileHandle::writePage(PageNum pageNum, const void *data) {
    try {
        if (currentFileHandled == NULL) {
            cout << "Current file handled is NULL!" << endl;
            return -1;
        }
        if ((pageNum < 0) || (pageNum >= getNumberOfPages())) {
            cout << "Illegal page number!" << endl;
            return -1;
        }
        if (fseek(currentFileHandled, PAGE_SIZE * pageNum, SEEK_SET) != 0) {
            cout << "Illegal position indicator!" << endl;
            return -1;
        }
        //size_t fwrite ( const void * ptr, size_t size, size_t count, FILE * stream );
        //Write block of data to stream
        //From the block of memory pointed by ptr, to the current position (SEEK_CUR) in the stream. (memory -> stream)
        //Writes an array of "count"(4096) elements, each one with a size of "size"(char is 1 byte) bytes
        //Internally, block pointed by ptr as if it was an array of unsigned char, then call fputc to write character to stream
        //Finally the position indicator of the stream is advanced by the total number of bytes written.
        size_t result = fwrite(data, sizeof(char), PAGE_SIZE, currentFileHandled);
        if (result != PAGE_SIZE) {
            cout << "Write error!" << endl;
            return -1;
        }
        //Finish write from buffer to file
        size_t flush = fflush(currentFileHandled);
        if (flush != 0) {
            cout << "Flush error!" << endl;
            return -1;
        }
        writePageCounter = writePageCounter + 1;
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

/**
 * Appends a new page to the end of the file, and writes the given data into the newly allocated page.
 *
 * @param data
 * @return
 */
RC FileHandle::appendPage(const void *data) {
    try {
        if (currentFileHandled == NULL) {
            cout << "Current file handled is NULL!" << endl;
            return -1;
        }
        //Reposition cursor: SEEK_END + 0 (SEEK_END: End of file) because only append page at the end
        if (fseek(currentFileHandled, 0, SEEK_END) != 0) {
            cout << "Illegal position indicator!" << endl;
            return -1;
        }
        size_t result = fwrite(data, sizeof(char), PAGE_SIZE, currentFileHandled);
        if (result != PAGE_SIZE) {
            cout << "Write error!" << endl;
            return -1;
        }
        //Finish write from buffer to file
        size_t flush = fflush(currentFileHandled);
        if (flush != 0) {
            cout << "Flush error!" << endl;
            return -1;
        }
        appendPageCounter = appendPageCounter + 1;
        return 0;
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}

/**
 * Return how many filled pages in the current file that the handler object is handling
 * @return
 */
unsigned FileHandle::getNumberOfPages() {
    try {
        struct stat fileStats;
        //The function fileno() examines the argument stream, and returns its integer descriptor.
        //The function fstat() put file info into stat object
        int result = fstat(fileno(currentFileHandled), &fileStats);
        if (result != 0) {
            cout << "Get file information error!" << endl;
            return -1;
        }
        return fileStats.st_size / PAGE_SIZE;  //st_size is in bytes, PAGE_SIZE is also in bytes
    } catch (const std::exception &e) {
        std::cout << e.what();
        return -1;
    } catch (const std::runtime_error &e) {
        std::cout << e.what();
        return -1;
    }
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
