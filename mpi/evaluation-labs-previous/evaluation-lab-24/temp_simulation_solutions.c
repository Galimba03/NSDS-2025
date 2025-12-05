#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// Group number:
// Group members:

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

    int local_n = n / size;
    double dx = L / (n - 1);
    double *u = malloc(local_n * sizeof(double));
    double *u_next = malloc(local_n * sizeof(double));

    // Set initial conditions
    for (int i = 0; i < local_n; i++) {
        double x = (rank * (n / size) + i) * dx;
        u[i] = initial_condition(x, L);
    }

    int round = 0;
    while (1) {
        // Perform one round of iterations
        round++;
        for (int t = 0; t < iterations_per_round; t++) {
            // Exchange values
            double prev, next = 0;
            if (rank > 0) {
                MPI_Send(&u[0], 1, MPI_DOUBLE, rank - 1, 0, MPI_COMM_WORLD);
                MPI_Recv(&prev, 1, MPI_DOUBLE, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            if (rank < size - 1) {
                MPI_Send(&u[local_n - 1], 1, MPI_DOUBLE, rank + 1, 0, MPI_COMM_WORLD);
                MPI_Recv(&next, 1, MPI_DOUBLE, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }

            // Compute the next values
            for (int i = 1; i < local_n - 1; i++) {
                u_next[i] = (u[i] + u[i - 1] + u[i + 1]) / 3;
            }
            u_next[0] = rank > 0 ? (prev + u[0] + u[1]) / 3 : (u[0] + u[1]) / 2;
            u_next[local_n - 1] = rank < size - 1 ? (u[local_n - 2] + u[local_n - 1] + next) / 3 : (u[local_n - 2] + u[local_n - 1]) / 2;

            // Update local state
            double *temp = u;
            u = u_next;
            u_next = temp;
        }

        // Compute local minimum and maximum
        double local_min = u[0];
        double local_max = u[0];
        for (int i=1; i<local_n; i++) {
            if (u[i] < local_min) {
                local_min = u[i];
            }
            if (u[i] > local_max) {
                local_max = u[i];
            }
        }

        // Compute global minimum and maximum
        double global_min, global_max, max_diff;
        MPI_Allreduce(&local_min, &global_min, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&local_max, &global_max, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        
        // Stopping condition
        max_diff = global_max - global_min;
        if (rank == 0) {
            printf("Round: %d\tMin: %f.5\tMax: %f.5\tDiff: %f.5\n", round, global_min, global_max, max_diff);
        }
        if (max_diff < allowed_diff) {
            break;
        }
    }

    // Deallocation
    free(u);
    free(u_next);

    MPI_Finalize();
    return 0;
}
