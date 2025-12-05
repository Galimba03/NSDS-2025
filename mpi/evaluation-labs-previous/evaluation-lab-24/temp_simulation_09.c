#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// Group number: 09
// Group members: Galimberti Matteo

const double L = 100.0;                 // Length of the 1d domain
const int n = 1000;                     // Total number of points
const int iterations_per_round = 1000;  // Number of iterations for each round of simulation
const double allowed_diff = 0.001;      // Stopping condition: maximum allowed difference between values

double initial_condition(double x, double L) {
    return fabs(x - L / 2);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // TODO
    // Variables declaration and initialization
    const int processor_array_size = n / size;
    int local_discrete_point = processor_array_size * (rank + 1);
    double *processor_array = (double*) malloc(processor_array_size * sizeof(double));
    
    double *next_processor_array = (double*) calloc(processor_array_size, sizeof(double));


    // TODO
    // Set initial conditions
    for (int i = 0; i < processor_array_size; i++) {
        processor_array[i] = initial_condition(local_discrete_point++, L);
    }

    int round = 0;

    double last_tail_temp = 0;
    double first_head_temp = 0;

    // Head is the left end side of the array
    // Tail is the right end side of the array
    int source_tail = (rank == 0) ? MPI_PROC_NULL : rank - 1;
    int dest_head = (rank == (size - 1)) ? MPI_PROC_NULL : rank + 1;

    int source_head = (rank == (size - 1)) ? MPI_PROC_NULL : rank + 1;
    int dest_tail = (rank == 0) ? MPI_PROC_NULL : rank - 1;

    while (1) {
        // Perform one round of iterations
        round++;
        for (int t = 0; t < iterations_per_round; t++) {
            // Outer arrays parameters:
            // exchange in the initial part of an array
            MPI_Sendrecv(&processor_array[processor_array_size - 1], 1, MPI_DOUBLE, dest_head, 0,
                 &last_tail_temp, 1, MPI_DOUBLE, source_tail, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            MPI_Sendrecv(&processor_array[0], 1, MPI_DOUBLE, dest_tail, 0,
                 &first_head_temp, 1, MPI_DOUBLE, source_head, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // TODO
            // Implement the code for each iteration
            for (int i = 0; i < processor_array_size; i++) {
                if (i == 0) {
                    if (rank == 0) {
                        next_processor_array[i] = (processor_array[i] + processor_array[i+1]) / 2;
                    } else {
                        next_processor_array[i] = (last_tail_temp + processor_array[i] + processor_array[i+1]) / 3;
                    }
                } else if (i == (processor_array_size - 1)) {
                    if (rank == (size - 1)) {
                        next_processor_array[i] = (processor_array[i-1] + processor_array[i]) / 2;
                    } else {
                        next_processor_array[i] = (processor_array[i-1] + processor_array[i] + first_head_temp) / 3;
                    }
                } else {
                    next_processor_array[i] = (processor_array[i-1] + processor_array[i] + processor_array[i+1]) / 3;
                }
            }

            double *temp = processor_array;
            processor_array = next_processor_array;
            next_processor_array = temp;
        }


        // TODO
        // Compute global minimum and maximum
        double global_min, global_max, max_diff;
        double local_max = processor_array[0], local_min = processor_array[0];
        
        for (int i = 1; i < processor_array_size; i++) {
            if (processor_array[i] > local_max) {
                local_max = processor_array[i];
            }
            
            if (processor_array[i] < local_min) {
                local_min = processor_array[i];
            }
        }

        // MPI_Reduce is a collective operation
        /*
            Deadlock in MPI_Reduce: You placed MPI_Reduce inside the if (rank == 0) block. MPI_Reduce is a collective operation; every process must call it, not just the root. If only rank 0 calls it, it waits forever for messages that other ranks never send.
        */
        MPI_Allreduce(&local_min, &global_min, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&local_max, &global_max, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

        max_diff = global_max - global_min;

        if (rank == 0) {
            printf("Round: %d\tMin: %f.5\tMax: %f.5\tDiff: %f.5\n", round, global_min, global_max, max_diff);
        }
        
        // TODO
        // Implement stopping conditions (break)
        /*
            Missing Synchronization for Stopping: Only rank 0 computes max_diff and decides whether to break the loop. The other processes don't know the result, so they continue the while loop and try to communicate with rank 0 (which has already finished). This causes a deadlock or crash. You must use MPI_Bcast to share the stopping condition.
         */
        if (max_diff < allowed_diff) {
            break;
        }
    }

    // TODO 
    // Deallocation
    free(processor_array);
    free(next_processor_array);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
