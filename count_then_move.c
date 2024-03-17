/*

*/

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h> 


#define N_THREADS 8 // t
#define N_HASHBITS 8 // b = 4 means 2**4 = 16 partitions (per thread)
#define N_PARTITIONS (1 << N_HASHBITS)
#define N_TUPLES 9999999 // N
#define TUPLESIZE 16
#define INPUT_BATCHSIZE 999 // change to 2**x




/*
*************************************************************************************************
*************************************************************************************************
*************************************************************************************************
************************** MAKE SURE TO PREVENT FALSE SHARING ***********************************
*************************************************************************************************
*************************************************************************************************
*************************************************************************************************
*/

struct Tuple {
    long long key;
    long long payload;
};

int hashFunction(long long x) {
    return x % (unsigned long long)N_PARTITIONBUFFERS; // TODO make Tuple.key always unsigned... this is a hack
}

struct partitionArgs {
    struct Tuple *partitionBuffers; 
    int partitionBufferSize;
    int n_items;
    FILE *inputF;
    int *indices;
};
void *partition(void *pArgs) 
{
    struct partitionArgs *args = (struct partitionArgs*)pArgs;

    int i = 0; 
    struct Tuple input_buffer[INPUT_BATCHSIZE];
    do {
        int batchsize;
        if(i + INPUT_BATCHSIZE > args->n_items) 
            batchsize = args->n_items - i;
        else 
            batchsize = INPUT_BATCHSIZE;

        // printf("attempting to read batch\n");
        int tmp = fread(&input_buffer, sizeof(struct Tuple), batchsize, args->inputF);
        if(tmp != batchsize) {
            fprintf(stderr, "fread didn't read enough bytes, maybe none");
            return NULL;
        }

        for(int j = 0; j < batchsize; j++) {
            int partition = hashFunction(input_buffer[j].key);
            int bufferIdx = (args->indices[partition])++;
            void *dest = args->partitionBuffers + (partition * args->partitionBufferSize) + bufferIdx;
            // printf("attempting memcpy\n"); // either dest doesn't exist or input_buffer[j] doesn't exist... 
            // // printf("dest key %lld payload %lld\n", *(long long*)dest, *(long long*)dest+sizeof(long long));
            // printf("dest key %lld payload %lld\n", *(long long*)dest, *((long long*)dest + 1));
            // printf("src %lld\n", input_buffer[j].key); 
            void *tmp = memcpy(dest, &input_buffer[j], sizeof(struct Tuple));
            if(tmp != dest) {
                fprintf(stderr, "memcpy failed");
                return NULL;
            }
            // printf("item number: %d input: %lld hash: %d\n", i+j, input_buffer[j].key, partition);
        }

        i += INPUT_BATCHSIZE;
    } while (i < args->n_items);


    return NULL;
} 

void countPartitionLengths(int *partitionLengths, int n_items, FILE *inputF) 
{
    int i = 0; 
    struct Tuple input_buffer[INPUT_BATCHSIZE];
    do {
        int batchsize;
        if(i + INPUT_BATCHSIZE > n_items) 
            batchsize = n_items - i;
        else 
            batchsize = INPUT_BATCHSIZE;

        int tmp = fread(&input_buffer, sizeof(struct Tuple), batchsize, inputF);
        if(tmp != batchsize) {
            fprintf(stderr, "fread didn't read enough bytes, maybe none");
            return NULL;
        }

        for(int j = 0; j < batchsize; j++) {
            int partition = hashFunction(input_buffer[j].key);
            (*partitionLengths[partition])++;
        }

        i += INPUT_BATCHSIZE;
    } while (i < n_items);
}

struct PartitionArgs {
    sem_t *slaveBarrier;
    sem_t *masterBarrier;
    int n_items;
    FILE *inputF; 
    int *partitionLengths;
    int *partitionOffsets;
    struct Tuple *buffer;
}
void *Partition(void *pArgs) 
{
    struct PartitionArgs *args = (struct PartitionArgs*) pArgs;

    countPartitionLengths(args->partitionLengths, args->n_items, args->inputF);

    sem_post(args->masterBarrier);
    sem_wait(args->slaveBarrier);

    int i = 0; 
    struct Tuple input_buffer[INPUT_BATCHSIZE];
    do {
        int batchsize;
        if(i + INPUT_BATCHSIZE > args->n_items) 
            batchsize = args->n_items - i;
        else 
            batchsize = INPUT_BATCHSIZE;

        int tmp = fread(&input_buffer, sizeof(struct Tuple), batchsize, args->inputF);
        if(tmp != batchsize) {
            fprintf(stderr, "fread didn't read enough bytes, maybe none");
            return NULL;
        }

        for(int j = 0; j < batchsize; j++) {
            int partition = hashFunction(input_buffer[j].key);
            void *dest = args->buffer + (partition * args->partitionOffsets[partition]) + i + j;
            void *tmp = memcpy(dest, &input_buffer[j], sizeof(struct Tuple));
            if(tmp != dest) {
                fprintf(stderr, "memcpy failed");
                return NULL;
            }
        }

        i += INPUT_BATCHSIZE;
    } while (i < args->n_items);


}

int main(int argc, char** argv) 
{
    printf("t: %d\nb:%d\n#tuples: %d\n", N_THREADS, N_HASHBITS, N_TUPLES);

    int itemsPerThread = (int)N_TUPLES / N_THREADS;

    struct Tuple *buffer = malloc(N_TUPLES * sizeof(struct Tuple));
    if(buffer == NULL) {
        fprintf(stderr, "malloc failed for buffer in main");
    }

    int (*partitionLengths)[N_THREADS][N_PARTITIONS]; // lengths of the partitions as counted pr thread
    partitionLengths = malloc(N_THREADS * N_PARTITIONS * sizeof(int));
    if (partitionLengths == NULL) {
        fprintf(stderr, "malloc failed for partitionlengths in main");
        free(buffer); 
        return 1;
    }

    int (*partitionOffsets)[N_THREADS][N_PARTITIONS];
    partitionOffsets = malloc(N_THREADS * N_PARTITIONS * sizeof(int));
    if (partitionOffsets == NULL) {
        fprintf(stderr, "malloc failed for partitionOffsets in main");
        free(buffer); 
        return 1;
    }

    sem_unlink("ctm_slaveBarrier");
    sem_t *slaveBarrier = sem_open(producerNames[bandidx], O_CREAT, 0660, 0); // only when main thread increments this by N_THREADS will the threads start writing to buffer
    if (slaveBarrier == SEM_FAILED) {
        printf("sem_open producer open failed!\n");
        return -1;
    }

    sem_unlink("ctm_masterBarrier");
    sem_t *masterBarrier = sem_open(producerNames[bandidx], O_CREAT, 0660, -(N_THREADS-1)); // only when every thread has incremented semaphore will this unblock
    if (masterBarrier == SEM_FAILED) {
        printf("sem_open producer open failed!\n");
        return -1;
    }

    pthread_t threads[N_THREADS];
    struct PartitionArgs pArgs[N_THREADS];
    for(int i = 0; i < N_THREADS; i++) {
        pArgs[i].slaveBarrier = slaveBarrier;
        pArgs[i].masterBarrier = masterBarrier; 
        if(i == N_THREADS - 1) 
            pArgs[i].n_items = N_TUPLES - itemsPerThread*(N_THREADS-1);
        else 
            pArgs[i].n_items = itemsPerThread;
        pArgs[i].inputF = fopen("random_integers.bin", "rb");
        if(pArgs[i].inputF == NULL) {
            fprintf(stderr, "Failed to open file\n");
            free(partitionBuffers);
            for(int j = i; j > -1; j--)
                fclose(pArgs[j].inputF);
            return 1;
        }
        fseek(pArgs[i].inputF, itemPerThread*i, SEEK_SET);
        pArgs[i].partitionLengths = &partitionLengths[i];
        pArgs[i].partitionOffsets = &partitionOffsets[i];

        pthread_create(&threads[i], NULL, partition, (void *)&pArgs[i]); 
    }

    sem_wait(masterBarrier); // wait for threads to have finished counting 

    // update offsets CLIFFHANGER THIS IS WRONG IT IS TRICKY SO THINK HARD !!!! 
    int runningOffset = 0; 
    for(int i = 0; i < N_PARTITIONS, i++) {
        int totalPartitionLength = 0; 
        for(int j = 0; j < N_THREADS; j++) 
            totalPartitionLength += partitionLengths[j][i]     
        for(int j = 0; j < N_THREADS; j++) 
            partitionOffsets[N_THREADS][i] = runningOffset + totalPartitionLength 
    }

    for(int i = 0; i < N_THREADS; i++) 
        sem_post(slaveBarrier);

    for(int i = 0; i < N_THREADS; i++) {
        pthread_join(threads[i], NULL);
        fclose(pArgs[i].inputF);
    }


    // VERIFICATION


    // free memory
    return 0;
}