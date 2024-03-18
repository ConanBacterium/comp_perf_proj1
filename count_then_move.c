/*

*/

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h> 
#include <fcntl.h>


#define N_THREADS 1 // t
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
    return x % (unsigned long long)N_PARTITIONS; // TODO make Tuple.key always unsigned... this is a hack
}

void countPartitionLengths(int *partitionLengths, int n_items, FILE *inputF) 
{
    printf("countPartitionLengths\n");

    long initialFilePos = ftell(inputF);

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
            return;
        }

        for(int j = 0; j < batchsize; j++) {
            int partition = hashFunction(input_buffer[j].key);
            partitionLengths[partition]++;
        }

        i += batchsize;
    } while (i < n_items);
    printf("finished counting\n");
    // for(int i = 0; i < 10; i++) {
    //     printf("partition %d count %d\n", i, partitionLengths[i]);
    // }
    fseek(inputF, initialFilePos, SEEK_SET); 
    printf("reset FD\n");
}

struct PartitionArgs {
    sem_t *slaveBarrier;
    sem_t *masterBarrier;
    int n_items;
    FILE *inputF; 
    int *partitionLengths;
    int *partitionOffsets;
    struct Tuple *buffer;
};
void *partition(void *pArgs) 
{
    struct PartitionArgs *args = (struct PartitionArgs*) pArgs;

    long initialFilePos = ftell(args->inputF);

    countPartitionLengths(args->partitionLengths, args->n_items, args->inputF);

    sem_post(args->masterBarrier);
    sem_wait(args->slaveBarrier);
    printf("finished waiting on slaveBarrier\n");

    // need to reset FD, no idea why but at this point the old one is corrupted
    printf("resetting FD to itialFilePos: %ld\n", initialFilePos);
    args->inputF = fopen("random_integers.bin", "rb");
    if(args->inputF == NULL) {
        fprintf(stderr, "couldn't reopen FD before second pass!!\n");
        return; 
    }
    fseek(args->inputF, initialFilePos, SEEK_SET); 
    printf("reset FD\n");

    int indices[N_PARTITIONS];
    memset(&indices, 0, sizeof(indices));

    int i = 0; 
    struct Tuple input_buffer[INPUT_BATCHSIZE];
    do {
        int batchsize;
        if(i + INPUT_BATCHSIZE > args->n_items) 
            batchsize = args->n_items - i;
        else 
            batchsize = INPUT_BATCHSIZE;
        // printf("attempt fread with batchsize: %d\n", batchsize);
        int tmp = fread(&input_buffer, sizeof(struct Tuple), batchsize, args->inputF);
        if(tmp != batchsize) {
            fprintf(stderr, "fread didn't read enough bytes, maybe none");
            return NULL;
        }
        // printf("fread succeeded\n");

        for(int j = 0; j < batchsize; j++) {
            int partition = hashFunction(input_buffer[j].key);
            int partitionIdx = indices[partition]++;
            void *dest = args->buffer + args->partitionOffsets[partition] + partitionIdx;
            // printf("attempting memcpy, partitionOffset %d i %d j %d (partition %d)\n", args->partitionOffsets[partition], i, j, partition);
            void *tmp = memcpy(dest, &input_buffer[j], sizeof(struct Tuple));
            // printf("succeeded memcpy\n");
            if(tmp != dest) {
                fprintf(stderr, "memcpy failed");
                return NULL;
            }
        }

        i += batchsize;
        // printf("%d of do while loop\n", i);
    } while (i < args->n_items);


}

int main(int argc, char** argv) 
{
    printf("t: %d\nb:%d\n#tuples: %d\n#partition: %d\n", N_THREADS, N_HASHBITS, N_TUPLES, N_PARTITIONS);

    int itemsPerThread = (int)N_TUPLES / N_THREADS;

    struct Tuple *buffer = malloc(N_TUPLES * sizeof(struct Tuple));
    if(buffer == NULL) {
        fprintf(stderr, "malloc failed for buffer in main");
    }

    int (*partitionLengths)[N_PARTITIONS]; // lengths of the partitions as counted pr thread
    partitionLengths = malloc(N_THREADS * N_PARTITIONS * sizeof(int));
    if (partitionLengths == NULL) {
        fprintf(stderr, "malloc failed for partitionlengths in main");
        free(buffer); 
        return 1;
    }

    int (*partitionOffsets)[N_PARTITIONS];
    partitionOffsets = malloc(N_THREADS * N_PARTITIONS * sizeof(int));
    if (partitionOffsets == NULL) {
        fprintf(stderr, "malloc failed for partitionOffsets in main");
        free(buffer); 
        return 1;
    }

    sem_unlink("ctm_slaveBarrier");
    sem_t *slaveBarrier = sem_open("ctm_slaveBarrier", O_CREAT, 0660, 0); // only when main thread increments this by N_THREADS will the threads start writing to buffer
    if (slaveBarrier == SEM_FAILED) {
        printf("sem_open slaveBarrier open failed!\n");
        return -1;
    }

    sem_unlink("ctm_masterBarrier");
    sem_t *masterBarrier = sem_open("ctm_masterBarrier", O_CREAT, 0660, 0); // only when every thread has incremented semaphore will this unblock
    if (masterBarrier == SEM_FAILED) {
        printf("sem_open masterBarrier open failed!\n");
        return -1;
    }

    printf("opened semaphores\n");

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
            free(buffer);
            free(partitionLengths);
            free(partitionOffsets);
            for(int j = i; j > -1; j--)
                fclose(pArgs[j].inputF);
            return 1;
        }
        fseek(pArgs[i].inputF, itemsPerThread*i, SEEK_SET);
        pArgs[i].partitionLengths = (int *)&partitionLengths[i];
        pArgs[i].partitionOffsets = (int *)&partitionOffsets[i];
        pArgs[i].buffer = buffer;

        pthread_create(&threads[i], NULL, partition, (void *)&pArgs[i]); 
    }

    for(int i = 0; i < N_THREADS; i++) {
        sem_wait(masterBarrier); // wait for all threads to sem_post the masterBarrier (be finished counting)
    }
    printf("master finished waiting for counts, will calculate offsets\n");

    // update offsets 
    int partitionEndIdx[N_PARTITIONS];
    int runningOffset = 0; 
    for(int i = 0; i < N_PARTITIONS; i++) {
        int totalPartitionLength = 0; 
        for(int j = 0; j < N_THREADS; j++) {
            totalPartitionLength += partitionLengths[j][i];
        }
        for(int j = 0; j < N_THREADS; j++) {
            partitionOffsets[j][i] = runningOffset;
            runningOffset += totalPartitionLength;
        } 
        partitionEndIdx[i] = runningOffset-1; // this is where partition i ends in the final buffer 
    }
    // for(int i = 0; i < 10; i++) {
    //     printf("partitionOffset %d: %d\n", i, partitionOffsets[0][i]);
    // }

    // NOTE: now runningOffset is the total length and partitionOffsets are the offsets of the partitionoffsets for each thread and finalPartitionOffsets are the offsets of the final buffer

    printf("master finished calculating offsets, will wake up slaves and wait for them to terminate\n");
    for(int i = 0; i < N_THREADS; i++) 
        sem_post(slaveBarrier); // slave threads will now write to the final buffer

    for(int i = 0; i < N_THREADS; i++) {
        pthread_join(threads[i], NULL);
        printf("master joined slave?\n");
        fclose(pArgs[i].inputF);
    }


    // VERIFICATION
    int runningIdx = 0; 
    for(int j = 0; j < N_PARTITIONS; j++) {
        for(int y = runningIdx; y < partitionEndIdx[j]+1; y++) {
            int partition = hashFunction(buffer[runningIdx].key);
            if(partition != j) {
                printf("wronk! %d runningIdx: %d\n", partition, runningIdx);
            }
            runningIdx++;
        }
    }

    // free memory
    return 0;
}