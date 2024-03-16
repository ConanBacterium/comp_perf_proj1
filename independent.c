/*
In the independent output technique, each thread has its
own output buffers for each output partition, i.e. t ∗ 2**b
output buffers. There is no sharing of output space between
threads and therefore no thread coordination required aside
from assigning input tuples to each thread. Each buffer requires 
the storage of metadata, such as the current writing
index and the size of the buffer. As the number of threads
or hash bits increases, the number of buffers required also
increases. At the same time, the expected size of each buffer, N/(t∗2**b)
, decreases, which means that the storage overhead associated with metadata increases.
The complete independence of each thread helps enable
parallelism by avoiding any contention between threads. 
Unintentional false sharing in the cache can be avoided 
by ensuring that 
!!! each thread’s buffer metadata does not share a !!!
!!! cache line with the metadata from another thread !!!
There are two main disadvantages of this approach. First,
the metadata overhead increases as additional threads or
partitions are used. And second, each partition is fragmented 
into t separate buffers. The operator that next processes 
the partition must either accept fragmented input or
a further consolidation step is needed.
*/

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#define N_THREADS 8 // t
#define N_HASHBITS 4 // b = 4 means 2**4 = 16 partition buffers (per thread)
#define N_PARTITIONBUFFERS (1 << N_HASHBITS) // bitwise left shift for exponentiation
#define N_TUPLES 9999999 // N
#define BUFFERSIZE (int)(N_TUPLES * 1.25 / ( N_THREADS * (N_PARTITIONBUFFERS) ) )  
#define TUPLESIZE 16
#define INPUT_BATCHSIZE 999 // change to 2**x

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

int main(int argc, char** argv) 
{
    printf("t: %d\nb:%d\n#tuples: %d\n#partitionbuffers: %d\nbuffersize: %d\n", N_THREADS, N_HASHBITS, N_TUPLES, N_PARTITIONBUFFERS, BUFFERSIZE);

    struct Tuple (*partitionBuffers)[N_PARTITIONBUFFERS][BUFFERSIZE]; // casting to pointer of size N_OUTBUFFERS * BUFFERSIZE. 
    long int partitionBuffers_size = N_THREADS * N_PARTITIONBUFFERS * BUFFERSIZE * sizeof(struct Tuple); 
    partitionBuffers = malloc(partitionBuffers_size);
    if(partitionBuffers == NULL) {
        fprintf(stderr, "Failed to malloc for partitionBuffers\n");
        return 1;
    }
    printf("Size of partitionBuffers array: %ld ( each tuple is 16 bytes so %ld mb)\n", partitionBuffers_size, partitionBuffers_size / 1000000);

    int indices[N_THREADS][N_PARTITIONBUFFERS]; 
    memset(&indices, 0, sizeof(indices));

    int itemsPerThread = (int)N_TUPLES / N_THREADS;

    pthread_t threads[N_THREADS];
    struct partitionArgs pArgs[N_THREADS];
    for(int i = 0; i < N_THREADS; i++) {
        pArgs[i].partitionBuffers = (struct Tuple*)&partitionBuffers[i]; 
        pArgs[i].partitionBufferSize = BUFFERSIZE;
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
        pArgs[i].indices = (int *)&indices[i];

        pthread_create(&threads[i], NULL, partition, (void *)&pArgs[i]); 
    }

    for(int i = 0; i < N_THREADS; i++) {
        pthread_join(threads[i], NULL);
        fclose(pArgs[i].inputF);
    }


    // VERIFICATION

    for(int i = 0; i < N_PARTITIONBUFFERS; i++) {
        // char filename[100];
        // snprintf(filename, sizeof(filename), "independent_partition_%d.bin", i);
        // FILE *outf = fopen(filename, "w");
        for(int j = 0; j < N_THREADS; j++) {
            for(int z = 0; z < indices[j][i]; z++) {
                int partition = hashFunction(partitionBuffers[j][i][z].key);
                if(partition != i) 
                    printf("WRONK\n");
                // fwrite(&partitionBuffers[j][i][z], sizeof(struct Tuple), 1, outf);
            }
        }
    }




    // free memory
    free(partitionBuffers);
    return 0;
}