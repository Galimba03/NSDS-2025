#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <mpi.h>

const int num_iter_per_proc = 100000000;

float generate_random_point() {
  return (float)rand() / (float)RAND_MAX;
}

int main() {
  MPI_Init(NULL, NULL);
    
  int rank;
  int num_procs;
  int sum;
  
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

  srand(time(NULL) + rank);

  
  int temp_sum = 0;
  float x, y;

  // Generation of the temporary value of pi
  for (int i = 0; i < num_iter_per_proc; i++) {
    x = generate_random_point();
    y = generate_random_point();
    if (x*x + y*y <= 1) {
      temp_sum++;
    }
  }

  if (rank == 0) {
    int temp_val;

    sum = temp_sum;
    // Wait each other process to process every partial_pi
    for(int i = 1; i < num_procs; i++) {
        // MPI_Recv(buffer, count, type, source, tag, comm, status)
        MPI_Recv(&temp_val, 1, MPI_LONG_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        sum += temp_val;
    }

    double pi = (4.0*sum) / (num_iter_per_proc*num_procs);
    printf("Pi = %f\n", pi);
  } else {
    // All other ranks send their value to rank 0
    MPI_Send(&temp_sum, 1, MPI_LONG_LONG, 0, 0, MPI_COMM_WORLD);
  }
    
  MPI_Finalize();
  return 0;
}
