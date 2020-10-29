#include <stdio.h>
#include <sys/mman.h>
#include <math.h>
#include <stdlib.h>
#include <zconf.h>
#include <pthread.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdint.h>


#define A_DATA_SIZE 313
#define B_ADRRESS 0xE46AE4C0
#define D_THREADS 89
#define RANDOM_SIZE 5000

#define E_OUTPUT_SIZE 55
#define G_BLOCK_SIZE 108

#define I_AGG_THREADS 135


typedef struct {
    int randomFD;
    unsigned char *address;
    int mBytes;
    int start;
    int end;
    pthread_t threadId;
} memory_writer_data;


typedef struct {
    int ints_per_file;
    int filesAmount;
    int *start;
    int *end;
} thread_writer_data;


typedef struct {
    int thread_number;
    int file_number;
} thread_reader_data;

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
    memory_writer_data *writeArgs = (struct WriteToMemoryArgs *) args;
    unsigned int bytesWrote = 0;
    for (int i = writeArgs->start; i < writeArgs->end; ++i) {
        unsigned char random = ReadChar(writeArgs->randomFD);
        writeArgs->address[i] = random;
        bytesWrote += 1;
    }

//    printf("%u\n", bytesWrote);
//    printf("%u %u\n", writeArgs->start, writeArgs->end);

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

void seq_write(void *ptr, int size, int n, int fd, const char *filepath) {
    struct stat fstat;
    stat(filepath, &fstat);
    int blksize = (int) fstat.st_blksize;
    int align = blksize - 1;
    int bytes = size * n;
//    невозможно использовать G, потому что флаг O_DIRECT требует,
//    чтобы адрес памяти и буфер были согласованы с размером
//    блока файловой системы =(
    int blocks = bytes / blksize;

    char *buff = (char *) malloc((int) blksize + align);
    // stackoverflow
    char *wbuff = (char *) (((uintptr_t) buff + align) & ~((uintptr_t) align));

    for (int i = 0; i < blocks; ++i) {
        char *buf_ptr = ptr + blksize * i;
        // copy from memory to write buffer
        for (int j = 0; j < blksize; j++) {
            buff[j] = buf_ptr[j];
        }
        if (pwrite(fd, wbuff, blksize, blksize * i) < 0) {
            free((char *) buff);
            printf("write error occurred\n");
            return;
        }
    }
    free((char *) buff);
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

        if (current_file_fd == -1) {
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


void *read_files(void *thread_data) {
    thread_reader_data *data = (thread_reader_data *) thread_data;

    char filename[6] = "lab1_0";
    filename[5] = '0' + data->file_number;
    int file_desc = -1;

    struct flock writeLock;
    memset(&writeLock, 0, sizeof(writeLock));
    while (file_desc < 0) {
        file_desc = open(filename, O_RDONLY, 00666);
    }
    writeLock.l_type = F_WRLCK;
    fcntl(file_desc, F_SETLKW, &writeLock);

    struct stat st;
    stat(filename, &st);
    int file_size = st.st_size;

    char *buffer = seq_read(file_desc, file_size);

    writeLock.l_type = F_UNLCK;
    fcntl(file_desc, F_SETLKW, &writeLock);
    close(file_desc);

    int *int_buf = (int *) buffer;
    long sum = 0;
    for (int i = 0; i < file_size / 4; i++) {
        sum += int_buf[i];
    }


//    printf("[READER-%d] file %s sum is %ld.\n", data->thread_number, filename, sum);

    free(buffer);
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



    // lseek() устанавливает указатель положения в файле,
    // указанном дескриптором handle,
    // в положение, указанное аргументами offset (0 (seek_set) - начало, 1 - текущая, 2 - конец)
    // и origin.
    if (lseek(outputFD, mBytes, SEEK_SET) < 0) {
        close(outputFD);
        perror("Error during calling lseek() to check availability");
        return 1;
    }

    if (write(outputFD, "", 1) < 0) {
        close(outputFD);
        perror("Error during writing last byte of the file");
        return 1;
    }

    char ch;
    while ((ch = getchar()) != '\n' && ch != EOF);
    printf("После ввода символа программа аллоцирует память\n");
    getchar();


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

    while ((ch = getchar()) != '\n' && ch != EOF);
    printf("После ввода символа программа будет записывать данные в память\n");
    getchar();

//    ================================
//    WRITE DATA TO MEMORY
//    ================================

    const int mem_part = mBytes / D_THREADS / sizeof(*address);
    const int mem_last = mBytes % D_THREADS / sizeof(*address);

//    https://habr.com/ru/post/326138/
    pthread_t writeToMemoryThreads[D_THREADS];
    memory_writer_data *args;
    int i;

    printf("Writing random data to memory...\n");
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
    elapsed = (double) (finish.tv_sec - start.tv_sec);
    elapsed += (double) (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("Writing in memory took %f seconds\n", elapsed);

    while ((ch = getchar()) != '\n' && ch != EOF);
    printf("После ввода символа программа будет записывать данные в файл\n");
    getchar();


//    ================================
//    WRITES DATA TO FILES
//    ================================


    printf("Writing random data to files...\n");
    clock_gettime(CLOCK_MONOTONIC, &start);

    int filesAmount = (A_DATA_SIZE / E_OUTPUT_SIZE) + 1;
    if (A_DATA_SIZE % E_OUTPUT_SIZE != 0) {
        filesAmount++;
    }

    printf("Files amount: %d\n", filesAmount);


    thread_writer_data *writer_data = (thread_writer_data *) malloc(sizeof(thread_writer_data));
    pthread_t *thread_writer = (pthread_t *) malloc(sizeof(pthread_t));
    writer_data->ints_per_file = E_OUTPUT_SIZE * 1024 * 256;
    writer_data->filesAmount = filesAmount;
    writer_data->start = map_ptr;
    writer_data->end = map_ptr + mBytes / 4;

    pthread_create(thread_writer, NULL, write_to_files, writer_data);
    pthread_join(*thread_writer, NULL);


    clock_gettime(CLOCK_MONOTONIC, &finish);
    elapsed = (double) (finish.tv_sec - start.tv_sec);
    elapsed += (double) (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("Writing to files took %f seconds\n", elapsed);

//    ================================
//    AGGREGATE DATA
//    ================================

    printf("Counting aggregating function...\n");
    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_t *reader_threads = (pthread_t *) malloc(I_AGG_THREADS * sizeof(pthread_t));
    thread_reader_data *reader_data = (thread_reader_data *) malloc(I_AGG_THREADS * sizeof(thread_reader_data));
    int file_number = 0;
    for (int i = 0; i < I_AGG_THREADS; ++i) {
        if (file_number >= filesAmount) {
            file_number = 0;
        }
        reader_data[i].thread_number = i;
        reader_data[i].file_number = file_number;
        file_number++;
    }

    for (int i = 0; i < I_AGG_THREADS; ++i) {
        pthread_create(&(reader_threads[i]), NULL, read_files, &reader_data[i]);
    }
    for (int i = 0; i < I_AGG_THREADS; i++) {
        pthread_join(reader_threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);
    elapsed = (double) (finish.tv_sec - start.tv_sec);
    elapsed += (double) (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("Aggregation took %f seconds\n", elapsed);


    if (munmap(map_ptr, mBytes) == -1) {
//        close(outputFD);
        close(randomFD);
        perror("Error un-mmapping the file");
        exit(EXIT_FAILURE);
    }

    while ((ch = getchar()) != '\n' && ch != EOF);
    printf("После ввода символа программа будет записывать данные в файл\n");
    getchar();

// Конец основного кода 0__0

    if (close(outputFD)) {
        printf("Error during file closing.\n");
    }

    if (close(randomFD)) {
        printf("Error in file closing.\n");
    }


    return 0;


}
