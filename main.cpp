////David Tanase, CSCE 313-200, Spring 2024
#include "pch.h"
#include <unordered_set>
#include <algorithm>
#define EOB 1

class MyBuf {
public:
    char* ptr; // pointer to buffer to search
    int size; // buffer size
    int slotID; // ID of the slot to return back
    UINT64 offset; // offset in the file (may be needed for debugging)
    bool first;
};

class HashValue {
public:
    DWORD counter;
    char* GetWordPtr(void) const { return (char*)(this + 1); }
};

class WordEntry {
public:
    DWORD counter;
    char* wordPointer;

    bool operator<(const WordEntry& other) const {
        if (counter == other.counter) {
            return strcmp(wordPointer, other.wordPointer) < 0;
        }
        else {
            return counter > other.counter;
        }
    }
};

#pragma pack(push, 1)
class HashHeader {
public:
    UINT64 hash;
    int next_offset;
};
#pragma pack(pop)

class MyQueue {
    char* buf;
    DWORD head;
    DWORD tail;
    DWORD bufSize;
    DWORD sizeQ;
    DWORD eSize;
    DWORD numSlots;
public:
    MyQueue() {
        eSize = 0;
        bufSize = 0;
        buf = nullptr;
        head = 0;
        tail = 0;
        sizeQ = 0;
        numSlots = 0;
    }

    MyQueue(DWORD elementSize, int size) {
        eSize = elementSize;
        bufSize = size * eSize;
        buf = (char*)calloc(size, eSize);
        head = 0;
        tail = 0;
        sizeQ = 0;
        numSlots = size;
    }

    ~MyQueue() {
        free(buf);
    }

    DWORD getSize() { return sizeQ; }

    void Push(void* item) {
        memcpy(buf + (tail * eSize), item, eSize);
        tail = (tail + 1) % numSlots;
        sizeQ++;
    }

    void Pop(void* item) {
        memcpy(item, buf + (head * eSize), eSize);
        head = (head + 1) % numSlots;
        sizeQ--;
    }
};

class PC {
public:
    HANDLE semaEmpty;
    HANDLE semaFull;
    CRITICAL_SECTION cs;

    HANDLE eventQuit;
    MyQueue* Q;

    HANDLE waitArray[2];

    DWORD eSize;

    PC(HANDLE eQ, DWORD size, DWORD elementSize) {
        semaEmpty = CreateSemaphore(NULL, size, size, NULL);
        if (semaEmpty == NULL) {
            printf("Failed to create semaFull with error code: %d", GetLastError());
            exit(-1);
        }

        semaFull = CreateSemaphore(NULL, 0, size, NULL);
        if (semaFull == NULL) {
            printf("Failed to create semaFull with error code: %d", GetLastError());
            exit(-1);
        }

        InitializeCriticalSection(&cs);

        Q = new MyQueue(elementSize, size);
        eventQuit = eQ;

        waitArray[0] = eventQuit;
        waitArray[1] = semaFull;

        eSize = elementSize;
    }

    ~PC() {
        if (semaEmpty != NULL) {
            CloseHandle(semaEmpty);
        }
        if (semaFull != NULL) {
            CloseHandle(semaFull);
        }

        DeleteCriticalSection(&cs);
    }

    void Produce(void* element) {
        if (WaitForSingleObject(semaEmpty, INFINITE) != WAIT_OBJECT_0) {
            printf("Failed to wait for semaEmpty with error code: %d", GetLastError());
        }
        EnterCriticalSection(&cs);
        Q->Push(element);
        LeaveCriticalSection(&cs);
        if (!ReleaseSemaphore(semaFull, 1, NULL)) {
            printf("Failed to release semaEmpty with error code: %d\n", GetLastError());
            exit(-1);
        }
    }
    
    int Consume(void* element) {
        DWORD waitResult = WaitForMultipleObjects(2, waitArray, FALSE, INFINITE);
        if (waitResult == WAIT_OBJECT_0) {
            return -1; //eventQuit signaled
        }
        else if (waitResult != WAIT_OBJECT_0 + 1) {
            printf("Failed to wait for semaFull with error code: %d", GetLastError());
            exit(-1);
        }
        EnterCriticalSection(&cs);
        Q->Pop(element);
        LeaveCriticalSection(&cs);
        if (!ReleaseSemaphore(semaEmpty, 1, NULL)) {
            printf("Failed to release semaFull with error code: %d\n", GetLastError());
        }
        return 0;
    }
};

LONGLONG getTime() {
    LARGE_INTEGER time;
    QueryPerformanceCounter(&time);
    return time.QuadPart;
}

char* formatNumber_DWORD(DWORD d) {
    if (d == 0) {
        char* cstr = new char[2];
        cstr[0] = '0';
        cstr[1] = '\0';
        return cstr;
    }

    std::string ret = "";
    int count = 0;

    while (d > 0) {
        if (count != 0 && count % 3 == 0) {
            ret += ',';
        }
        ret += (d % 10) + '0';
        d /= 10;
        count++;
    }

    std::string temp = "";

    for (int i = ret.size() - 1; i >= 0; i--) {
        temp += ret[i];
    }

    char* cstr = new char[temp.size() + 1];
    memcpy(cstr, temp.c_str(), temp.size() + 1);

    return cstr;
}

char* formatNumber(UINT64 d) {
    if (d == 0) {
        char* cstr = new char[2];
        cstr[0] = '0';
        cstr[1] = '\0';
        return cstr;
    }

    std::string ret = "";
    int count = 0;

    while (d > 0) {
        if (count != 0 && count % 3 == 0) {
            ret += ',';
        }
        ret += (d % 10) + '0';
        d /= 10;
        count++;
    }

    std::string temp = "";

    for (int i = ret.size() - 1; i >= 0; i--) {
        temp += ret[i];
    }

    char* cstr = new char[temp.size() + 1];
    memcpy(cstr, temp.c_str(), temp.size() + 1);

    return cstr;
}

class HashTable {
public:
    int* hash;
    char* mainHashBuf;

    UINT64 offset;
    UINT64 capacity;

    int nBins;
    int size;

    int upperToLower[256];

    UINT64 max_depth = 0;
    UINT64 lookup_total = 0;
    UINT64 searches = 0;

    HashTable(int nB) {
        nBins = nB;
        size = 0;

        hash = (int*)malloc(nBins * sizeof(int));
        memset(hash, -1, nBins * sizeof(int));

        mainHashBuf = (char*)VirtualAlloc(NULL, (UINT64) 1 << 40, MEM_RESERVE, PAGE_READWRITE);
        mainHashBuf = (char*)VirtualAlloc(mainHashBuf, (UINT64) 1 << 20, MEM_COMMIT, PAGE_READWRITE);

        offset = 0;
        capacity = 1 << 20;

        for (int i = 0; i < 256; ++i) {
            upperToLower[i] = 0;
        }

        for (char c = 'a'; c <= 'z'; ++c) {
            upperToLower[c] = c;
        }

        for (char c = 'A'; c <= 'Z'; ++c) {
            upperToLower[c] = c + 32;
        }
    }

    HashValue* FindInsertKey(UINT64 hashKey, int valueSize, bool& found) {
        DWORD hash_slot = hashKey & (nBins - 1);
        lookup_total++;
        searches++;
        int temp_searches = 1;

        //if hash offset is -1, instantiate to current offset and create hash entry
        if (hash[hash_slot] == -1) {
            hash[hash_slot] = offset;

            if (offset + sizeof(HashHeader) + valueSize >= capacity) {
                VirtualAlloc(mainHashBuf + capacity, (UINT64) 1 << 20, MEM_COMMIT, PAGE_READWRITE);
                capacity += 1 << 20;
            }

            HashHeader* curr_hH = (HashHeader*) (mainHashBuf + offset);
            curr_hH->hash = hashKey;
            curr_hH->next_offset = -1;
            found = false;

            offset += sizeof(HashHeader) + valueSize;
            size++;

            return (HashValue*)(curr_hH + 1);
        }

        else {
            //if hash offset != -1, iterate through collision chain until word is found
            HashHeader* curr_hH = (HashHeader*) (mainHashBuf + hash[hash_slot]);
            HashValue* curr_hV = (HashValue*) (curr_hH + 1);
            while(curr_hH->next_offset != -1) {
                searches++;
                temp_searches++;
                if (temp_searches > max_depth) {
                    max_depth = temp_searches;
                }
                if (hashKey == curr_hH->hash) {
                    found = true;
                    return curr_hV;
                }
                else {
                    curr_hH = (HashHeader*) (mainHashBuf + curr_hH->next_offset);
                    curr_hV = (HashValue*) (curr_hH + 1);
                }
            }
            if (hashKey == curr_hH->hash) {
                found = true;
                return curr_hV;
            }
            else { //if next pointer is -1 and word doesnt equal
                if (offset + sizeof(HashHeader) + valueSize >= capacity) {
                    VirtualAlloc(mainHashBuf + capacity, (UINT64)1 << 20, MEM_COMMIT, PAGE_READWRITE);
                    capacity += 1 << 20;
                }

                curr_hH->next_offset = offset;

                HashHeader* new_hH = (HashHeader*) (mainHashBuf + offset);
                new_hH->hash = hashKey;
                new_hH->next_offset = -1;
                found = false;
                size++;

                offset += sizeof(HashHeader) + valueSize;

                return (HashValue*)(new_hH + 1);
            }
        }
    }

    void PrintContents(FILE* file) {
        WordEntry* printBuf = new WordEntry[size];
        int printBufIndex = 0;

        for (int i = 0; i < nBins; i++) {
            if (hash[i] != -1) {
                HashHeader* curr_hH = (HashHeader*)(mainHashBuf + hash[i]);
                HashValue* curr_hV = (HashValue*)(curr_hH + 1);
                while (curr_hH->next_offset != -1) {
                    printBuf[printBufIndex].counter = curr_hV->counter;
                    printBuf[printBufIndex++].wordPointer = curr_hV->GetWordPtr();

                    curr_hH = (HashHeader*)(mainHashBuf + curr_hH->next_offset);
                    curr_hV = (HashValue*)(curr_hH + 1);
                }
                printBuf[printBufIndex].counter = curr_hV->counter;
                printBuf[printBufIndex++].wordPointer = curr_hV->GetWordPtr();
            }
        }

        for (int i = 0; i < size; i++) {
            toLower(printBuf[i].wordPointer);
        }

        std::sort(printBuf, printBuf + size);

        for (int i = 0; i < size; i++) {
            fprintf(file, "[%s] %s = %s\n", formatNumber(i),  printBuf[i].wordPointer, formatNumber_DWORD(printBuf[i].counter));
            //fprintf(file, "%s\n", formatNumber_DWORD(printBuf[i].counter));
        }

        delete[] printBuf;
    }

    void toLower(char* cstr) {
        int i = 0;
        while(cstr[i] != '\0') {
            cstr[i] = upperToLower[cstr[i]];
            i++;
        }
    }

    void tallyWordLengths() {
        DWORD* countBuf = new DWORD[31];
        memset(countBuf, 0, sizeof(DWORD) * 31);
        int printBufIndex = 0;

        for (int i = 0; i < nBins; i++) {
            if (hash[i] != -1) {
                HashHeader* curr_hH = (HashHeader*)(mainHashBuf + hash[i]);
                HashValue* curr_hV = (HashValue*)(curr_hH + 1);
                while (curr_hH->next_offset != -1) {
                    countBuf[strlen(curr_hV->GetWordPtr())]++;

                    curr_hH = (HashHeader*)(mainHashBuf + curr_hH->next_offset);
                    curr_hV = (HashValue*)(curr_hH + 1);
                }
                countBuf[strlen(curr_hV->GetWordPtr())]++;
            }
        }

        for (int i = 3; i <= 31; i++) {
            printf("%lu\n", countBuf[i]);
        }
    }
};

class MainThreadClass {
public:
    HANDLE terminateEvent;
    HANDLE timerEvent;
    CRITICAL_SECTION cs;
    CPU cpu;
    MersenneTwister mt;

    PC* pcEmpty;
    PC* pcFull;

    UINT64 fileSize;
    DWORD lenLongestWord = 32;
    DWORD nSlots = 0;
    int slotSize = 0;
    DWORD B = 0;
    int shadowSize = 0;
    int padding = 0;

    char* mega_buf;
    char* filename;

    FILE* file;

    int activeThreads = 0;
    UINT64 totalBytesRead = 0;
    UINT64 totalMatches = 0;

    UINT64 total_words = 0;
    UINT64 unique_words = 0;
    UINT64 invalid_words = 0;

    UINT64 num_lookups;
    UINT64 lookup_depth;
    UINT64 max_lookup_depth;

    char isalphaLUT[256];
    char isdelimiterLUT[256];
    UINT64 sboxLUT[256];

    HashTable* main_hT;

    LONGLONG init_mergeTime;
    LONGLONG final_mergeTime;

    int nB;

    MainThreadClass(int nBin, int buffer_size, char* fn, FILE* f) {
        terminateEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
        timerEvent = CreateEvent(NULL, TRUE, TRUE, NULL);
        InitializeCriticalSection(&cs);

        if (terminateEvent == NULL)
        {
            printf("CreateEvent error: %d\n", GetLastError());
            exit(-1);
        }

        if (timerEvent == NULL)
        {
            printf("CreateEvent error: %d\n", GetLastError());
            exit(-1);
        }

        file = f;
        filename = fn;
        lenLongestWord = 32;

        nSlots = cpu.cpus + 5; // num slots to maintain
        pcEmpty = new PC(terminateEvent, nSlots, sizeof(int));
        pcFull = new PC(terminateEvent, nSlots, sizeof(MyBuf));

        DWORD sectorSize = 0;
        GetDiskFreeSpace(NULL, NULL, &sectorSize, NULL, NULL);
        shadowSize = (lenLongestWord / sectorSize + 1) * sectorSize;
        padding = shadowSize + sectorSize; // both shadow buffers
        B = 1 << buffer_size; // 1MB in each slot
        slotSize = B + padding; // full slot with padding
        // VirtualAlloc guarantees page-aligned addresses, while the heap does not
        mega_buf = (char*)VirtualAlloc(NULL, (UINT64)nSlots * slotSize, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);

        for (int i = 0; i < 256; ++i) {
            isalphaLUT[i] = 0;
            isdelimiterLUT[i] = 0;
        }
        
        for (char c = 'a'; c <= 'z'; ++c) {
            isalphaLUT[c] = 1;
        }
        
        for (char c = 'A'; c <= 'Z'; ++c) {
            isalphaLUT[c] = 1;
        }

        isdelimiterLUT['\0'] = 1;
        isdelimiterLUT[' '] = 1;
        isdelimiterLUT[','] = 1;
        isdelimiterLUT['\n'] = 1;
        isdelimiterLUT['\r'] = 1; 
        isdelimiterLUT['.'] = 1;
        isdelimiterLUT['\''] = 1;
        isdelimiterLUT['"'] = 1;
        isdelimiterLUT['?'] = 1;
        isdelimiterLUT['-'] = 1;
        isdelimiterLUT[':'] = 1;
        isdelimiterLUT[';'] = 1;
        isdelimiterLUT['*'] = 1;
        isdelimiterLUT['!'] = 1;
        isdelimiterLUT['\t'] = 1;

        for (int i = 0; i < 256; i++) {
            sboxLUT[i] = mt.genrand64_int64();
        }

        for (char c = 'A'; c <= 'Z'; ++c) {
            sboxLUT[c] = sboxLUT[c + 32];
        }

        nB = nBin;
        main_hT = new HashTable(nB);
    };

    void ProcessData();
    void DiskRead();
    void TrackStats();

    int FindNextWordStart(MyBuf cb, int off, DWORD* wordStart);
    int FindThisWordEnd(MyBuf cb, DWORD wordStart, DWORD* wordEnd, UINT64* hashKey);
    bool WordIsEligible(MyBuf cb, DWORD wordStart, DWORD wordEnd);
};

void MainThreadClass::DiskRead() {
    char* prevShadowBuffer = (char*)malloc(lenLongestWord);

    for (int i = 0; i < nSlots; i++) {
        pcEmpty->Produce(&i);
    }

    HANDLE hFile = CreateFile(filename, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        printf("CreateFile error: %d\n", GetLastError());
        exit(-1);
    }

    DWORD high, low = GetFileSize(hFile, &high);
    if (low == INVALID_FILE_SIZE) {
        printf("GetFileSize error: %d\n", GetLastError());
        exit(-1);
    }
    fileSize = ((UINT64)high << 32) + low;

    totalBytesRead = 0;
    bool reachedEof = false;
    bool first = true;

    int slotID = 0;
    while (!reachedEof) {
        if (pcEmpty->Consume(&slotID) == -1) {
            return;
        }

        DWORD bytesRead = 0;
        char* currBuf = mega_buf + (slotID * slotSize);
        if (ReadFile(hFile, currBuf+shadowSize, B, &bytesRead, NULL) == FALSE) {
            if (GetLastError() != ERROR_HANDLE_EOF) {
                printf("ReadFile error: %d\n", GetLastError());
                exit(-1);
            }
            reachedEof = true;
        }
        else if (bytesRead < B) {
            reachedEof = true;
        }

        EnterCriticalSection(&cs);
        totalBytesRead += bytesRead;
        LeaveCriticalSection(&cs);

        //printf("bytes read: %d\n", bytesRead);
        memcpy(currBuf + shadowSize - lenLongestWord, prevShadowBuffer, lenLongestWord);
        memcpy(prevShadowBuffer, currBuf + shadowSize + B - lenLongestWord, lenLongestWord);

        MyBuf mb;
        if (first && !reachedEof) {
            mb.ptr = currBuf + shadowSize;
            mb.size = bytesRead - lenLongestWord + 1;
            mb.first = true;

            mb.ptr[-1] = '\0';

            first = false;
        }
        else if (!reachedEof) {
            mb.ptr = currBuf + shadowSize - lenLongestWord;
            mb.size = bytesRead + 1;
            mb.first = false;
        }
        else if (first && reachedEof) {
            mb.ptr = currBuf + shadowSize;
            mb.size = bytesRead + 1;
            mb.first = true;

            mb.ptr[-1] = '\0';

            first = false;
        }
        else {
            mb.ptr = currBuf + shadowSize - lenLongestWord;
            mb.size = bytesRead + lenLongestWord;
            mb.first = false;
        }
        mb.slotID = slotID;
        mb.offset = 0;

        char* nullCharSlot = currBuf + shadowSize + bytesRead;
        *nullCharSlot = '\0';

        pcFull->Produce(&mb);
    }

    init_mergeTime = getTime();

    int slot = 0;
    for (int i = 0; i < nSlots; i++) {
        pcEmpty->Consume(&slot);
    }
    SetEvent(terminateEvent);
    
    final_mergeTime = getTime();
}

bool strcompare(char* s1, char* s2) {
    while (*s1 != '\0') {
        if (*s1 != *s2) {
            return false;
        }
        s1++;
        s2++;
    }
    return true;
}

unsigned int hashStr(char* str) {
    unsigned char* s = (unsigned char*)str;
    return ((unsigned int)(s[0]) << 16) + ((unsigned int)(s[1]) << 8) + (unsigned int)s[2];
}

int MainThreadClass::FindNextWordStart(MyBuf cb, int off, DWORD* wordStart) {
    char* buf = cb.ptr;
    DWORD buf_size = cb.size;

    while (off < buf_size) {
        if (isalphaLUT[(unsigned char)buf[off]] == 1) {
            *wordStart = off;
            return 0;
        }
        off++;
    }

    return EOB;
}

int MainThreadClass::FindThisWordEnd(MyBuf cb, DWORD wordStart, DWORD* wordEnd, UINT64* hashKey) {
    char* buf = cb.ptr;
    DWORD buf_size = cb.size;
    DWORD curr = wordStart;
    int index = 0;
  
    while (buf[curr] != '\0') {
        if (!isalphaLUT[(unsigned char)buf[curr]]) {
            *wordEnd = curr;
            return 0;
        }
        *hashKey = (*hashKey + sboxLUT[(UCHAR)buf[curr]]) * 3;
        curr++;
    }

    return EOB;
}

bool MainThreadClass::WordIsEligible(MyBuf cb, DWORD wordStart, DWORD wordEnd) {
    char* buf = cb.ptr;

    int len = wordEnd - wordStart;
    if (len < 3 || len > 31) {
        return false;
    }

    if (isdelimiterLUT[(unsigned char)buf[(int)wordStart - 1]] != 1 || isdelimiterLUT[(unsigned char)buf[(int)wordEnd]] != 1) {
        return false;
    }

    return true;
}

void MainThreadClass::ProcessData() {
    MyBuf cb;
    DWORD wordStart;
    DWORD wordEnd;
    DWORD wordLen;
    UINT64 hashKey;
    HashTable* local_HT = new HashTable(nB);

    while (pcFull->Consume(&cb) != -1) {
        int off = 0;
        DWORD t_words = 0;
        DWORD i_words = 0;
        DWORD wordStart = 0;
        DWORD wordEnd = 0;

        if (!cb.first) {
            if (FindThisWordEnd(cb, wordStart, &wordEnd, &hashKey) == EOB) {
                pcEmpty->Produce(&cb.slotID);
                EnterCriticalSection(&cs);
                invalid_words += i_words;
                total_words += t_words;
                activeThreads--;
                LeaveCriticalSection(&cs);
                continue;
            }
            off = wordEnd + 1;
        }
            
        while (off < cb.size) {
            if (FindNextWordStart(cb, off, &wordStart) == EOB) {
                break;
            }

            hashKey = 0;
            if (FindThisWordEnd(cb, wordStart, &wordEnd, &hashKey) == EOB) {
                i_words++;
                t_words++;
                break;
            }
            wordLen = wordEnd - wordStart;
            if (WordIsEligible(cb, wordStart, wordEnd)) {
                bool found;
                int valueSize = sizeof(HashValue) + wordLen + 1;
                HashValue* hv = local_HT->FindInsertKey(hashKey, valueSize, found);
                if (found) {
                    hv->counter++;
                }
                else {
                    hv->counter = 1;
                    memcpy(hv->GetWordPtr(), cb.ptr + wordStart, wordLen);
                    char* nullChar = hv->GetWordPtr() + wordLen;
                    *nullChar = '\0';
                }
            }
            else {
                i_words++;
            }
            t_words++;
            off = wordEnd + 1;
        }

        pcEmpty->Produce(&cb.slotID);
        EnterCriticalSection(&cs);
        invalid_words += i_words;
        total_words += t_words;
        if (local_HT->max_depth > max_lookup_depth) {
            max_lookup_depth = local_HT->max_depth;
        }
        lookup_depth = local_HT->searches;
        num_lookups = local_HT->lookup_total;
        activeThreads--;
        LeaveCriticalSection(&cs);
    }

    EnterCriticalSection(&cs);

    DWORD count = 0;
    char* wordPtr = nullptr;
    bool found;
    int valueSize;
    int wL;

    for (int i = 0; i < local_HT->nBins; i++) {
        if (local_HT->hash[i] != -1) {
            HashHeader* curr_hH = (HashHeader*)(local_HT->mainHashBuf + local_HT->hash[i]);
            HashValue* curr_hV = (HashValue*)(curr_hH + 1);
            while (curr_hH->next_offset != -1) {
                DWORD count = curr_hV->counter;
                char* wordPtr = curr_hV->GetWordPtr();

                found = false;
                wL = strlen(wordPtr);
                valueSize = sizeof(HashValue) + wL + 1;
                hashKey = curr_hH->hash;
                HashValue* hv = main_hT->FindInsertKey(hashKey, valueSize, found);
                if (found) {
                    hv->counter += count;
                }
                else {
                    hv->counter = count;
                    memcpy(hv->GetWordPtr(), wordPtr, wL);
                    char* nullChar = hv->GetWordPtr() + wL;
                    *nullChar = '\0';
                }

                curr_hH = (HashHeader*)(local_HT->mainHashBuf + curr_hH->next_offset);
                curr_hV = (HashValue*)(curr_hH + 1);
            }
            count = curr_hV->counter;
            wordPtr = curr_hV->GetWordPtr();

            found = false;
            wL = strlen(wordPtr);
            valueSize = sizeof(HashValue) + wL + 1;
            hashKey = curr_hH->hash;
            HashValue* hv = main_hT->FindInsertKey(hashKey, valueSize, found);
            if (found) {
                hv->counter += count;
            }
            else {
                hv->counter = count;
                memcpy(hv->GetWordPtr(), wordPtr, wL);
                char* nullChar = hv->GetWordPtr() + wL;
                *nullChar = '\0';
            }
        }
    }

    LeaveCriticalSection(&cs);

    return;
}

void MainThreadClass::TrackStats() {
    UINT64 lastBytes = 0;
    while (true) {
        DWORD dwWaitResult = WaitForSingleObject(terminateEvent, 2000);
        if (dwWaitResult == WAIT_OBJECT_0) {
            break;
        }

        double cpu_util = cpu.GetCpuUtilization(NULL);
        int ram_util = cpu.GetProcessRAMUsage(true);

        double readSpeed = (totalBytesRead - lastBytes) / 2.0;
        printf("[%.1f%%] %.2f MB/s, words %.1fM, depth (%.3f, %d), [CPU %.0f%% RAM %d MB]\n",
            (totalBytesRead / (float)(fileSize)) * 100,
            (readSpeed) / 1000000.0,
            total_words / 1000000.0,
            (double)lookup_depth / num_lookups, max_lookup_depth,
            cpu_util, ram_util);
        fprintf(file, "[%.1f%%] %.2f MB/s, words %.1fM, depth (%.3f, %d), [CPU %.0f%% RAM %d MB]\n",
            (totalBytesRead / (float)(fileSize)) * 100,
            (readSpeed) / 1000000.0,
            total_words / 1000000.0,
            (double)lookup_depth / num_lookups, max_lookup_depth,
            cpu_util, ram_util);

        lastBytes = totalBytesRead;
    }
}

class ThreadParams {
public:
    int threadID; // Thread sequence number between 0 and MAX_THREADS-1
    MainThreadClass* lpMTC; // Pointer to a shared version of the class
};

DWORD WINAPI InitializeThread(LPVOID p) {
    ThreadParams* t = (ThreadParams*)p;

    //SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_IDLE);

    if (t->threadID == 0) {
        t->lpMTC->TrackStats();
    }
    else if (t->threadID == 1) {
        t->lpMTC->DiskRead();
    }
    else {
        SetThreadAffinityMask(GetCurrentThread(), 1 << (t->threadID - 2));
        t->lpMTC->ProcessData();
    }

    return 0;
}

int main(int argc, char* argv[]) {
    //Check Sysargs
    if (argc != 3) {
        printf("(-) Usage: <buf_size> <wikiversion.txt>\n");
        return 1;
    }

    FILE* file = fopen("report.txt", "w");

    if (file == nullptr) {
        perror("Error opening file");
        exit(-1);
    }
  
    CPU cpu;
    DWORD K = cpu.cpus; //Num cores

    //NUM BINS
    int num_bins = 1 << 20;

    //Initialize Threads
    HANDLE* threadHandles = new HANDLE[K+2];
    ThreadParams* t = new ThreadParams[K+2];
    MainThreadClass mtc(num_bins, atoi(argv[1]), argv[2], file);

    for (int i = 0; i < K+2; i++) {
        t[i].threadID = i;
        t[i].lpMTC = &mtc;

        if ((threadHandles[i] = CreateThread(NULL, 0, InitializeThread, &t[i], 0, NULL)) == NULL) {
            printf("(-) Error %d creating thread.", GetLastError());
            exit(-1);
        }
    }

    LONGLONG initTime = getTime();

    //Wait For Thread Termination
    for (int i = 0; i < K+2; i++)
    {
        if (WaitForSingleObject(threadHandles[i], INFINITE) == WAIT_FAILED) {
            printf("(-) Error %d waiting for thread termination.", GetLastError());
        }
        if (CloseHandle(threadHandles[i]) == 0) {
            printf("(-) Error %d closing thread handle.", GetLastError());
        }
    }

    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    double total_delta = (double)(getTime() - initTime) / frequency.QuadPart;
    double total_merge_delta = (double)(mtc.final_mergeTime - mtc.init_mergeTime) / frequency.QuadPart;

    //Termination Procedures
    fprintf(file, "\nMerge delay: %.0f ms\n", total_merge_delta * 1000);
    fprintf(file, "Execution time: %.2f sec, %.1f MB/s, %.1fM wps\n", total_delta, (mtc.fileSize/total_delta)/1000000, (mtc.total_words/total_delta)/1000000);
    printf("\nMerge delay: %.0f ms\n", total_merge_delta * 1000);
    printf("Execution time: %.2f sec, %.1f MB/s, %.1fM wps\n", total_delta, (mtc.fileSize / total_delta) / 1000000, (mtc.total_words / total_delta) / 1000000);
    printf("\nUnique: %s\n", formatNumber(mtc.main_hT->size));
    printf("Invalid: %s\n", formatNumber(mtc.invalid_words));
    printf("Total: %s\n", formatNumber(mtc.total_words));
    fprintf(file, "\nUnique: %s\n", formatNumber(mtc.main_hT->size));
    fprintf(file, "Invalid: %s\n", formatNumber(mtc.invalid_words));
    fprintf(file, "Total: %s\n\n", formatNumber(mtc.total_words));
    mtc.main_hT->PrintContents(file);

    fclose(file);

    //mtc.main_hT->tallyWordLengths();

    ShellExecute(NULL, "open", "report.txt", NULL, NULL, SW_SHOWNORMAL);
}
