#include <mpi.h>
#include <stdio.h>

int main(int argc, char** argv) {
  // Init the MPI environment
  // Passing NULL, NULL simply means we aren't passing command-line arguments to the MPI implementation itself.
  MPI_Init(NULL, NULL);

  // "How many of us are there?"
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // "Who am I?"
  // This fills world_rank with a unique ID (integer) for this specific process. Ranks range from 0 to world_size - 1.
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Get the name of the processor
  // The processor could run on a different physical computer. This get the name of the computer the process is running on.
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Print off a hello world message
  printf("Hello world from processor %s (rank %d out of %d)\n", processor_name, world_rank, world_size);

  // Finalize the MPI environment
  MPI_Finalize();
}
