#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

// Creates an array of random numbers.
int *create_random_array(int num_elements, int max_value) {
    int *arr = (int *)malloc(sizeof(int) * num_elements);
    for (int i = 0; i < num_elements; i++) {
        arr[i] = (rand() % max_value);
    }
    return arr;
}

float compute_average(int *array, int num_elements) {
    int sum = 0;
    for (int i = 0; i < num_elements; i++) {
        sum += array[i];
    }
    return ((float)sum) / num_elements;
}

float compute_final_average(float *array, int num_elements) {
    float sum = 0.0f;
    for (int i = 0; i < num_elements; i++) {
        sum += array[i];
    }
    return sum / num_elements;
}

// Process 0 selects a number num.
// All other processes have an array that they filter to only keep the elements
// that are multiples of num.
// Process 0 collects the filtered arrays and print them.
int main(int argc, char **argv)
{
    // Maximum value for each element in the arrays
    const int max_val = 100;
    // Number of elements for each processor
    int num_elements_per_proc = 50;
    // Number to filter by
    int num_to_filter_by = 2;
    if (argc > 1) {
        num_elements_per_proc = atoi(argv[1]);
    }

    // Init random number generator
    srand(time(NULL));

    MPI_Init(NULL, NULL);

    int my_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // 1. Preparation
    int *global_arr = NULL;
    if (my_rank == 0) {
        // global_arr is an array of elements of dimention "num_elements_per_proc * world_size"
        global_arr = create_random_array(num_elements_per_proc * world_size, max_val);
    }

    // Allocate a buffer for the local subset of data on EACH process
    int *local_arr = (int *)malloc(sizeof(int) * num_elements_per_proc);

    // 2. Scatter: Distribute chunks of the array to everyone
    MPI_Scatter(
        global_arr,             // 1. The Big Array (Root only)
        num_elements_per_proc,  // 2. Items PER PROCESS. This is the number of elements sent to each process, not the total size of the array.
        MPI_INT,
        local_arr,              // 3. The Small Array (Everyone)
        num_elements_per_proc,  // 4. Items PER PROCESS. How many elements this process expects to receive.
        MPI_INT,                
        0,                      // 5. Who is distributing? -> int root
        MPI_COMM_WORLD);

    // 3. Local Computation
    float local_avg = compute_average(local_arr, num_elements_per_proc);
    printf("Rank %d computed average: %f\n", my_rank, local_avg);

    // 4. Gather preparation
    float *all_avgs = NULL;
    if (my_rank == 0) {
        all_avgs = (float *)malloc(sizeof(float) * world_size);
    }

    // 5. Gather: Collect individual averages back to Rank 0
    MPI_Gather(
        &local_avg,     // 1. Local Data (Everyone)
        1,              // 2. Items sent by THIS process
        MPI_FLOAT,
        all_avgs,       // 3. The Big Buffer (Root only)
        1,              // 4. Items received PER PROCESS
        MPI_FLOAT,
        0,              // 5. Who is collecting?
        MPI_COMM_WORLD  
    );

    // 6. Final Computation
    if (my_rank == 0) {
        float final_avg = compute_final_average(all_avgs, world_size);
        printf("Global average: %f\n", final_avg);
        free(all_avgs);
        free(global_arr);
    }

    free(local_arr);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}