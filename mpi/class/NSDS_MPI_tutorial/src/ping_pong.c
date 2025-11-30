#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// Simple ping pong program to exemplify MPI_Send and MPI_Recs
// Assume only two processes
int main(int argc, char** argv) {
  const int tot_msgs = 100;
  
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  int num_msgs = 0;
  int partner_rank = (my_rank + 1) % 2;

  while (num_msgs < tot_msgs) {
    if (my_rank == 0) {
      // Rank 0 increments and sends first
      num_msgs++;
      MPI_Send(&num_msgs, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD);
      printf("Rank %d sent %d\n", my_rank, num_msgs);

      // Then waits for the return
      MPI_Recv(&num_msgs, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("Rank %d received %d\n", my_rank, num_msgs);
    } else {
      // Rank 1 waits first
      MPI_Recv(&num_msgs, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("Rank %d received %d\n", my_rank, num_msgs);

      // Then increments and sends back
      num_msgs++;
      MPI_Send(&num_msgs, 1, MPI_INT, partner_rank, 0, MPI_COMM_WORLD);
      printf("Rank %d sent %d\n", my_rank, num_msgs);
    }
  }
  
  MPI_Finalize();
}
