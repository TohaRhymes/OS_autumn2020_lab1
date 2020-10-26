#include <stdio.h>
#include <sys/mman.h>
#include <math.h>
//#include <fcntl.h>
#include <stdlib.h>
#include <zconf.h>
#include <pthread.h>
#include <string.h>

#define O_DIRECT    00040000    /* direct disk access hint */

#include <fcntl.h>
#include <sys/stat.h>
#include <stdint.h>


#define A_DATA_SIZE 313
#define B_ADRRESS 0xE46AE4C0
#define D_THREADS 89
//#define D_THREADS 4

#define E_OUTPUT_SIZE 55
#define G_BLOCK_SIZE 108

#define I_AGG_THREADS 135


struct WriteToMemoryArgs {
    int randomFD;
    unsigned char *address;
    int mBytes;
    int start;
    int end;
    pthread_t threadId;
};


typedef struct {
    int ints_per_file;
    int filesAmount;
    int *start;
    int *end;
} thread_writer_data;

#define RANDOM_SIZE 10000
int randomByteIndex;
char randomChar[RANDOM_SIZE];

unsigned char ReadChar(int randomFD) {
    if (randomByteIndex >= RANDOM_SIZE) {
        size_t result = read(randomFD, &randomChar, sizeof(randomChar));
        if (result == -1) {
            perror("ERROR WITH /dev/urandom\n");
            return 0;
        }
        randomByteIndex = 0;
    }
    return randomChar[randomByteIndex++];
}

void *WriteToMemory(void *args) {
    struct WriteToMemoryArgs *writeArgs = (struct WriteToMemoryArgs *) args;
    unsigned int bytesWrote = 0;
    for (int i = writeArgs->start; i < writeArgs->end; ++i) {
        unsigned char random = ReadChar(writeArgs->randomFD);
        writeArgs->address[i] = random;
        bytesWrote += 1;
    }

    printf("%u\n", bytesWrote);
    printf("%u %u\n", writeArgs->start, writeArgs->end);

#ifdef LOG
    printf("Thread with ID %lu finished and wrote %u bytes\n", writeArgs->threadId, bytesWrote);
#endif
    free(writeArgs);
}

char *seq_read(int fd, int file_size) {
    char *buffer = (char *) malloc(file_size);
    int blocks = file_size / G_BLOCK_SIZE;
    int last_block_size = file_size % G_BLOCK_SIZE;
    char *buf_ptr;
    for (int i = 0; i < blocks; ++i) {
        buf_ptr = buffer + G_BLOCK_SIZE * i;
        pread(fd, buf_ptr, G_BLOCK_SIZE, G_BLOCK_SIZE * i);
    }
    if (last_block_size > 0) {
        buf_ptr = buffer + G_BLOCK_SIZE * blocks;
        pread(fd, buf_ptr, last_block_size, G_BLOCK_SIZE * blocks);
    }
    return buffer;
}

void seq_write(void *ptr, int size, int n, int fd, const char* filepath) {
    struct stat fstat;
    stat(filepath, &fstat);
    int blksize = (int) fstat.st_blksize;
    int align = blksize-1;
    int bytes = size * n;
    // impossible to use G from the task because O_DIRECT flag requires aligned both the memory address and your buffer to the filesystem's block size
    int blocks = bytes / blksize;

    char *buff = (char *) malloc((int)blksize+align);
    // stackoverflow
    char *wbuff = (char *)(((uintptr_t)buff+align)&~((uintptr_t)align));

    for (int i = 0; i < blocks; ++i) {
        char* buf_ptr = ptr + blksize*i;
        // copy from memory to write buffer
        for (int j = 0; j < blksize; j++) {
            buff[j] = buf_ptr[j];
        }
        if (pwrite(fd, wbuff, blksize, blksize*i) < 0) {
            free((char *)buff);
            printf("write error occurred\n");
            return;
        }
    }
    free((char *)buff);
}

void *write_to_files(void *thread_data) {
    thread_writer_data *data = (thread_writer_data *) thread_data;
    int *write_pointer = data->start;

    for (int i = 0; i < data->filesAmount; i++) {
        char filename[6] = "lab1_0";
        filename[5] = '0' + i;

        struct flock readLock;
        memset(&readLock, 0, sizeof(readLock));

        // NOCACHE file write
        int current_file_fd = open(filename, O_WRONLY | O_CREAT | __O_DIRECT, 00666);

        readLock.l_type = F_RDLCK;
        fcntl(current_file_fd, F_SETLKW, &readLock);

        if (current_file_fd == - 1) {
            printf("error on open file for write\n");
            return NULL;
        }
        int ints_to_file = data->ints_per_file;
        int is_done = 0;
        while (!is_done) {
            if (ints_to_file + write_pointer < data->end) {
                seq_write(write_pointer, sizeof(int), ints_to_file, current_file_fd, filename);
                write_pointer += ints_to_file;
                is_done = 1;
            } else {
                int available = data->end - write_pointer;
                seq_write(write_pointer, sizeof(int), available, current_file_fd, filename);
                write_pointer = data->start;
                ints_to_file -= available;
            }
        }
        readLock.l_type = F_UNLCK;
        fcntl(current_file_fd, F_SETLKW, &readLock);
        close(current_file_fd);

    }
    return NULL;
}


int main() {

    int mBytes = A_DATA_SIZE * pow(10, 6); // size in megabytes
    unsigned char *address = (unsigned char *) B_ADRRESS; // starting address

    // nocache  = (mode_t) 040000
    int outputFD = open("output", O_RDWR | O_TRUNC | O_CREAT, (mode_t) 0777);
    if (outputFD < 0) {
        perror("Can't open file\n");
        return 1;
    }

    int randomFD = open("/dev/urandom", O_RDONLY);
    if (randomFD < 0) {
        perror("Can't read /dev/urandom\n");
        return 1;
    }


    //todo CHECKKK

    // lseek() устанавливает указатель положения в файле,
    // указанном дескриптором handle,
    // в положение, указанное аргументами offset (0 (seek_set) - начало, 1 - текущая, 2 - конец)
    // и origin.
    if (lseek(outputFD, mBytes, SEEK_SET) == -1) {
        close(outputFD);
        perror("Error during calling lseek() to check availability");
        return 1;
    }

    if (write(outputFD, "", 1) == -1) {
        close(outputFD);
        perror("Error during writing last byte of the file");
        return 1;
    }

    // void * mmap (void *address, size_t length, int protect, int flags, int filedes, off_t offset)
    // PROT (protection): the access types of read, write and execute are the permissions on the content
    // MAP:
    //  MAP_SHARED: share the mapping with all other processes, which are mapped to this object; changes will be written back to the file.
    //  MAP_PRIVATE: the mapping will not be seen by any other processes, and the changes made will not be written to the file.
    //  MAP_ANONYMOUS / MAP_ANON: the mapping is not connected to any files; is used as the basic primitive to extend the heap.
    //  MAP_FIXED: the system has to be forced to use the exact mapping address specified in the address.
    int *map_ptr = mmap(address, mBytes,
                        PROT_READ | PROT_WRITE,
                        MAP_SHARED,
                        outputFD, 0);
    if (map_ptr == MAP_FAILED) {
        perror("Mapping Failed\n");
        return 1;
    }

// Тут основной код 0__0
    srand(time(NULL));
//-------------------------------------WRITES RANDOM DATA TO MEMORY-------------------------------------
    const int mem_part = mBytes / D_THREADS / sizeof(*address);
    const int mem_last = mBytes % D_THREADS / sizeof(*address);

//    https://habr.com/ru/post/326138/
    pthread_t writeToMemoryThreads[D_THREADS];
    struct WriteToMemoryArgs *args;
    int i;

    struct timespec start, finish;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);

    for (i = 0; i < D_THREADS; ++i) {
        args = malloc(sizeof(*args));
        args->address = address;
        args->randomFD = randomFD;
        args->mBytes = mBytes;

        args->start = i * mem_part;
        args->end = (i + 1) * mem_part;
        args->threadId = writeToMemoryThreads[i];
//        int pthread_create(pthread_t *thread,
//                           const pthread_attr_t *attr,
//                           void *(*start)(void *),
//                           void *arg);
//        arg — это бестиповый указатель, содержащий аргументы потока.
//        первый параметр - адрес для хранения идентификатора создаваемого потока типа pthread_t.
//        attr - бестиповый указатель атрибутов потока pthread_attr_t; NULL, то поток создается с атрибутами по умолчанию.
//        start - указателем на потоковую void * функцию, принимающей 1 переменную - бестиповый указатель.
        if (pthread_create(&writeToMemoryThreads[i], NULL, WriteToMemory, (void *) args)) {
            free(args);
            perror("Can't create thread");
        }
//        printf("%d %d\n", i, &(address)[i]);

    }


    if (mem_last > 0) {
        args = malloc(sizeof(*args));
        args->address = address;
        args->randomFD = randomFD;
        args->mBytes = mBytes;

        args->start = i * mem_part;
        args->end = i * mem_part + mem_last;
        WriteToMemory((void *) args);
    }

    for (i = 0; i < D_THREADS; ++i) {
        pthread_join(writeToMemoryThreads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);

    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    printf("Writing in memory took %f seconds\n", elapsed);

//    while ((ch = getchar()) != '\n' && ch != EOF);
//    printf("After input char program will continue writing data to file\n");
//    getchar();
//-------------------------------------WRITES RANDOM DATA TO FILES-------------------------------------

    int filesAmount = (A_DATA_SIZE / E_OUTPUT_SIZE) + 1;
    if (A_DATA_SIZE % E_OUTPUT_SIZE != 0) {
        filesAmount++;
    }
//#ifdef LOG
    printf("Files amount: %d\n", filesAmount);
//#endif

    thread_writer_data *writer_data = (thread_writer_data *) malloc(sizeof(thread_writer_data));
    pthread_t *thread_writer = (pthread_t *) malloc(sizeof(pthread_t));
    writer_data->ints_per_file = E_OUTPUT_SIZE * 1024 * 256;
    writer_data->filesAmount = filesAmount;
    writer_data->start = map_ptr;
    writer_data->end = map_ptr + mBytes / 4;

    pthread_create(thread_writer, NULL, write_to_files, writer_data);
    pthread_join(*thread_writer, NULL);

//    int files[filesAmount];
//    sem_t *fileSems = malloc(sizeof(sem_t) * filesAmount);
//    CleanFiles(filesAmount, files);
//    OpenFiles(filesAmount, files);
//    printf("Here\n");
//    pthread_t writeToFilesThreadId;
//
//    for (i = 0; i < filesAmount; i++) {
//        sem_init(&fileSems[i], 0, 1);
//    }
//
//    struct WriteToFilesArgs *writeToFilesArgs = malloc(sizeof(struct WriteToFilesArgs));
//
////    clock_gettime(CLOCK_MONOTONIC, &start);
//
//    int fileSizeRemainder = bytes / (filesAmount);
//    int fileSizeQuotient = bytes % (filesAmount);
//
//    writeToFilesArgs->fileSizeQuotient = fileSizeQuotient;
//    writeToFilesArgs->fileSizeRemainder = fileSizeRemainder;
//    writeToFilesArgs->files = files;
//    writeToFilesArgs->fileSems = fileSems;
//    writeToFilesArgs->filesAmount = filesAmount;
//    writeToFilesArgs->memoryRegion = memoryRegion;
//
//    WriteToFilesOnce(writeToFilesArgs);
//
//
//    /* l_type   l_whence  l_start  l_len  l_pid   */
//    struct flock fl = {F_WRLCK, SEEK_SET, 0, 0, 0};
//    int fd;
//
//    fl.l_pid = getpid();
//
//    if ((fd = open("lockdemo.c", O_RDWR)) == -1) {
//        perror("open");
//        exit(1);
//    }
//
//    printf("Press <RETURN> to try to get lock: ");
//    getchar();
//    printf("Trying to get lock...");
//
//    if (fcntl(fd, F_SETLKW, &fl) == -1) {
//        perror("fcntl");
//        exit(1);
//    }
//
//    printf("got lock\n");
//    printf("Press <RETURN> to release lock: ");
//    getchar();
//
//    fl.l_type = F_UNLCK;  /* set to unlock same region */
//
//    if (fcntl(fd, F_SETLK, &fl) == -1) {
//        perror("fcntl");
//        exit(1);
//    }
//
//    printf("Unlocked.\n");
//
//    close(fd);



// Конец основного кода 0__0



    if (close(outputFD)) {
        printf("Error during file closing.\n");
    }

    if (close(randomFD)) {
        printf("Error in file closing.\n");
    }


    return 0;

//    printf("Size of pointer: %d\n", sizeof(ptr));
//    ptr[0] = 5;
//    printf("%d", ptr);
//    printf("%d", ptr + 1);


    char myRandomData[50];
    size_t randomDataLen = 0;
    while (randomDataLen < sizeof myRandomData) {
        ssize_t result = read(randomFD, myRandomData + randomDataLen, (sizeof myRandomData) - randomDataLen);
        if (result < 0) {
            // something went wrong
        }
        randomDataLen += result;
    }
    close(outputFD);
    close(randomFD);






//
//
//
//    for(int i=0; i<N; i++){
//        printf("[%d] ",ptr[i]);
//    }
//
//    printf("\n");
//    int err = munmap(ptr, 10*sizeof(int));
//
//    if(err != 0){
//        printf("UnMapping Failed\n");
//        return 1;
//    }
//    return 0;
}
