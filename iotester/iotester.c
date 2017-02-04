#include <stdlib.h>
#include <stdio.h>
#include <string.h>

int read(char *path, int size)
{
    FILE* fp;

    fp = fopen(path, "r");
    if(fp == NULL)
    {
        printf("File does \"%s\" does not exist\n", path);
        return -1;
    }

    void *buffer = malloc(131072); // 128 kilobytes;

    if(buffer == NULL)
        return -2;

    for(int i = 0; i < size * 8; i++)
    {
        int read = fread(buffer, 1, 131072, fp);

        if(read != 131072)
        {
            fclose(fp);
            free(buffer);
            printf("What?\n");
            return -1;
        }
    }

    fclose(fp);
    free(buffer);

    return 0;
}

int append(char *path, int size)
{
    FILE* fp;

    fp = fopen(path, "a");
    if(fp == NULL)
    {
        printf("Someting went wrong appending %s", path);
        return -1;
    }

    void *buffer = malloc(131072); // 128 kilobytes;

    if(buffer == NULL)
        return -2;

    for(int i = 0; i < size * 8; i++)
    {
        int written = fwrite(buffer, 1, 131072, fp);

        if(written != 131072)
        {
            fclose(fp);
            free(buffer);
            printf("What?\n");
            return -1;
        }
    }

    fclose(fp);
    free(buffer);

    return 0;
}

int main(int argc, char** argv)
{
    if(argc < 4)
    {
        printf("Invalid number of arguments: ./iotester cmd path size\n");
        return -1;
    }

    if(! strcmp("read", argv[1]))
        return read(argv[2], atoi(argv[3]));
    else if(! strcmp("append", argv[1]))
        return append(argv[2], atoi(argv[3]));
    else
    {
        printf("Unknown mode, use read/append\n");
        return -1;
    }

    return -1;
}
