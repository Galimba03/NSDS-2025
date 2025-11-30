#include <mpi.h>
#include <stdio.h>

// Run with two processes.
// Process 0 sends an integer to process 1 and vice-versa.
// Try to run the system: what goes wrong?

/*
  Standard MPI_Send: Might copy the data into a buffer and return immediately, allowing the code to proceed (unsafe to rely on, but possible).

  Synchronous MPI_Ssend: It is a strict handshake. The function blocks (waits) until the destination process has actually posted a matching MPI_Recv and started receiving the data.
*/
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  int msg_to_send = 1;
  int msg_to_recv;
  MPI_Ssend(&msg_to_send, 1, MPI_INT, other_rank, 0, MPI_COMM_WORLD);
  MPI_Recv(&msg_to_recv, 1, MPI_INT, other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  
  MPI_Finalize();
}

/*
  How to fix it

  As per your preference for programming topics, I will explain this in English.

This code illustrates a classic Deadlock scenario caused by Synchronous Communication.

While the logic looks symmetric and correct (I send to you, then I receive from you), the specific use of MPI_Ssend guarantees that the program will freeze forever.

1. The Culprit: MPI_Ssend

The "S" stands for Synchronous.

    Standard MPI_Send: Might copy the data into a buffer and return immediately, allowing the code to proceed (unsafe to rely on, but possible).

    Synchronous MPI_Ssend: It is a strict handshake. The function blocks (waits) until the destination process has actually posted a matching MPI_Recv and started receiving the data.

2. The Step-by-Step Failure

Here is what happens when you run this with 2 processes:

    Rank 0 executes MPI_Ssend(target: 1). It stops and waits, thinking: "I cannot continue until Rank 1 calls Receive."

    Rank 1 executes MPI_Ssend(target: 0). It stops and waits, thinking: "I cannot continue until Rank 0 calls Receive."

Both processes are stuck at line 14 (MPI_Ssend). Neither can reach line 15 (MPI_Recv) to unblock the other. It is a circular dependency.

3. How to fix it

  A. Reorder the calls (Break the symmetry)
  Make one process send first, and the other receive first.
  
  if (my_rank == 0) {
      MPI_Ssend(...); // Sends first
      MPI_Recv(...);
  } else {
      MPI_Recv(...);  // Receives first (unblocks Rank 0)
      MPI_Ssend(...);
  }

  B. Use MPI_Sendrecv
  MPI provides a specialized function that handles the "Send and Receive" pattern simultaneously without deadlocking.

  MPI_Sendrecv(
      &msg_to_send, 1, MPI_INT, other_rank, 0,
      &msg_to_recv, 1, MPI_INT, other_rank, MPI_ANY_TAG,
      MPI_COMM_WORLD, MPI_STATUS_IGNORE
  );
  
  C. Use Non-Blocking Calls (MPI_Isend)
  Start the send operation but don't wait for it to finish immediately.

  MPI_Request req;
  // Start sending in background
  MPI_Isend(&msg_to_send, 1, MPI_INT, other_rank, 0, MPI_COMM_WORLD, &req);
  // Immediately go to receive
  MPI_Recv(&msg_to_recv, 1, MPI_INT, other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  // Clean up the send
  MPI_Wait(&req, MPI_STATUS_IGNORE);

*/