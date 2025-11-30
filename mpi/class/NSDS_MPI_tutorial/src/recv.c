#include <mpi.h>
#include <stdio.h>

// Every process p>0 waits for an interger from process p=0 that never arrives.
// Try to check the use of resources.
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  // Getting the rank of the process (its ID)
  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  // If the rank is not equal to 0 (the actual process is not the master process)
  if (my_rank > 0) {
    int buf;
    // Bloking receive
    // We are waiting 1 message that contains an MPI_INT from the source rank of 0
    MPI_Recv(&buf, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  MPI_Finalize();
}

/*
  int MPI_Recv(
    void *buf,             // 1. Where to store the data
    int count,             // 2. Maximum number of items
    MPI_Datatype datatype, // 3. The type of data
    int source,            // 4. Rank of the sender
    int tag,               // 5. Message label/ID
    MPI_Comm comm,         // 6. The communicator
    MPI_Status *status     // 7. Status object for details
  );

  int MPI_Send(
    const void *buf,       // 1. Data to send
    int count,             // 2. Number of items
    MPI_Datatype datatype, // 3. The type of data
    int dest,              // 4. Rank of the receiver
    int tag,               // 5. Message label/ID
    MPI_Comm comm          // 6. The communicator
  );
*/