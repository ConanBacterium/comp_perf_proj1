#include <stdio.h>
#include <limits.h>
#include "dbg.h"

#define INPUT_SIZE = 268435456
#define INPUT_SIZE_T = 16
#define MAX_BITS = 18

int getInput(int *dest) 
{
    FILE *inputf = fopen("random_integers.bin", "r");

    int nItemsRead = fread(dest, INPUT_SIZE, INPUT_SIZE_T, inputf); 
    if( nItemsRead != INPUT_SIZE) {
        fclose(inputf); 
        return -1;
    }

    fclose(inputf); 
    return 1;
}