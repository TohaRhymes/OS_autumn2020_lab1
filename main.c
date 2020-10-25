#include <stdio.h>
#include <sys/mman.h>
#include <math.h>
#include <fcntl.h>
#include <stdlib.h>
#include <zconf.h>

#define A_DATA_SIZE 313
#define B_ADRRESS 0xE46AE4C0
#define D_THREADS 89
#define E_output_size

int main() {

    int bytes = A_DATA_SIZE * pow(10, 6); // size in megabytes
    unsigned char *address = (unsigned char *) B_ADRRESS; // starting address

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

    // void * mmap (void *address, size_t length, int protect, int flags, int filedes, off_t offset)
    // PROT (protection): the access types of read, write and execute are the permissions on the content
    // MAP:
    //MAP_SHARED: share the mapping with all other processes, which are mapped to this object; changes will be written back to the file.
    //MAP_PRIVATE: the mapping will not be seen by any other processes, and the changes made will not be written to the file.
    //MAP_ANONYMOUS / MAP_ANON: the mapping is not connected to any files; is used as the basic primitive to extend the heap.
    //MAP_FIXED: the system has to be forced to use the exact mapping address specified in the address.
    int *ptr = mmap(address, bytes,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    outputFD, 0);
    if (ptr == MAP_FAILED) {
        perror("Mapping Failed\n");
        return 1;
    }

    printf("Size of pointer: %d\n", sizeof(ptr));
    ptr[0] = 5;
    printf("%d", ptr);
    printf("%d", ptr+1);



    char myRandomData[50];
    size_t randomDataLen = 0;
    while (randomDataLen < sizeof myRandomData)
    {
        ssize_t result = read(randomFD, myRandomData + randomDataLen, (sizeof myRandomData) - randomDataLen);
        if (result < 0)
        {
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
