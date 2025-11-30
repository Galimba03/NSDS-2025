#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// The receiver does not know in advance the number of messages it will receive (and the source sending them).
// It dynamically gets the number of messages using a probe.
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  // Sender
  if (my_rank == 0) {
    int num_msgs = 10;

    int *msgs = (int *) malloc(sizeof(int) * num_msgs);
    for (int i=0; i<num_msgs; i++) {
      msgs[i] = i;
    }
    
    MPI_Send(msgs, num_msgs, MPI_INT, other_rank, 0, MPI_COMM_WORLD);
    free(msgs);
  }
  // Receiver
  else {
    MPI_Status status;

    // Probe allows checking the message without retrieving it
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    int count;
    // Extract the number of elements from the status
    MPI_Get_count(&status, MPI_INT, &count);

    int *recv_buf = (int *) malloc(sizeof(int) * count);

    // Receive the actual data using the known count
    MPI_Recv(recv_buf, count, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    printf("Rank %d received %d dynamic elements.\n", my_rank, count);
    
    free(recv_buf);
  }
  
  MPI_Finalize();
}
