#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEBUG 0

int rank;
int num_procs;

const int num_rounds = 1000;

const int min_num = 1;
const int max_num = 1000;

// Array, one element per process
// The leader board, instantiated and used in process 0
int *leaderboard = NULL;

// Array, one element per process
// The array of number selected in the current round
int *selected_numbers = NULL;

// The leader for the current round
int leader = 0;

// Allocate dynamic variables
void allocate_vars() {
  selected_numbers = (int *) malloc(num_procs * sizeof(int));
  if (rank == 0) {
    leaderboard = (int *) malloc(num_procs * sizeof(int));
    memset((void *) leaderboard, 0, num_procs * sizeof(int));
  }
}

// Deallocate dynamic variables
void free_vars() {
  free(selected_numbers);
  if (rank == 0) {
    free(leaderboard);
  }
}

// Select a random number between min_num and max_num
int select_number() {
  return min_num + rand() % (max_num - min_num + 1);
}

// Function used to communicate the selected number to the leader
void send_num_to_leader(int num) {
  // I am the leader
  if (rank == leader) {
    // Write the number in my position
    selected_numbers[rank] = num;
    // Receive from all other processes
    for (int i=0; i<num_procs-1; i++) {
      /*
        The MPI_Status structure is primarily used to retrieve information about a message when you use wildcards (like MPI_ANY_SOURCE or MPI_ANY_TAG) or when you need to know the actual size of the received data.
        There are 3 direct fields you can access and 1 calculated value (the count).
        
        1. Direct Fields (Access via .)
        You can access these directly like any C struct member:
        - status.MPI_SOURCE
          - What it is: The rank ID of the process that sent the message.
          - When to use: Crucial when you used MPI_ANY_SOURCE in your Receive or Probe call. You need this to know who to reply to or where to store the data.

        - status.MPI_TAG
          - What it is: The integer tag attached to the message.
          - When to use: Useful if you used MPI_ANY_TAG to accept different types of messages (e.g., Tag 0 for "Data", Tag 1 for "Stop"). You check this field to decide how to handle the logic.

        - status.MPI_ERROR
          - What it is: The error code of the operation.
          - When to use: Rarely used in standard code because the default behavior of MPI is to crash the program immediately if an error occurs. Unless you changed the error handler, you won't check this.

        2. The Hidden Field (The Count)
          This is the most common confusion. status.count does not exist.
          To find out how many elements were received, you must pass the status structure to a helper function: MPI_Get_count.
      */
      MPI_Status status;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      int source = status.MPI_SOURCE;
      int tag = status.MPI_TAG;
      MPI_Recv(&selected_numbers[source], 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
  }
  // I am not the leader
  else {
    MPI_Send(&num, 1, MPI_INT, leader, 0, MPI_COMM_WORLD);
  }
}

/*
  MPI_Probe + Loop (The Solution Approach)
    - Best for: Unbalanced workloads, unknown message sizes, or "Master-Worker" patterns where workers finish at different times.
    - Pros: Handles messages First-Come-First-Served; allows checking message metadata before receiving.
    - Cons: High overhead (latency) for checking Probe repeatedly; verbose code.
*/

// Compute the winner (-1 if there is no winner)
// Function invoked by the leader only
int compute_winner(int number_to_guess) {
  #if DEBUG
  printf("Number to guess: %d\n", number_to_guess);
  for (int i=0; i<num_procs; i++) {
    printf("P%d selected %d\n", i, selected_numbers[i]);
  }
  #endif
  
  // Compute the minimum distance
  int min_distance = max_num;
  for (int i=0; i<num_procs; i++) {
    int distance = abs(selected_numbers[i]-number_to_guess);
    if (distance < min_distance) {
      min_distance = distance;
    }
  }
  
  // Compute the process with the minimum distance
  int winner = -1;
  for (int i=0; i<num_procs; i++) {
    if (abs(selected_numbers[i]-number_to_guess) == min_distance) {
      // This is the winner
      if (winner < 0) {
	      winner = i;
      }
      // There was already a process at the same distance: no winner for this round
      else {
        winner = -1;
        break;
      }
    }
  }
  return winner;
}

// Function used to communicate the winner to everybody
void send_winner(int *winner) {
  // The current leader broadcasts the winner
  MPI_Bcast(winner, 1, MPI_INT, leader, MPI_COMM_WORLD);
}

// Update leader
void update_leader(int winner) {
  // If there is a winner, change the leader
  if (winner >= 0) {
    leader = winner;
  }
}

// Update leaderboard (invoked by process 0 only)
void update_leaderboard(int winner) {
  // Process 0 updates the leaderboard
  if (winner >= 0) {
    leaderboard[winner]++;
  }
}

// Print the leaderboard
void print_leaderboard(int round, int winner) {
  printf("\n* Round %d *\n", round);
  printf("Winner: %d\n", winner);
  printf("Leaderboard\n");
  for (int i=0; i<num_procs; i++) {
    printf("P%d:\t%d\n", i, leaderboard[i]);
  }
}

int main(int argc, char** argv) { 
  MPI_Init(NULL, NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  srand(time(NULL) + rank);

  allocate_vars();

  for (int round=0; round<num_rounds; round++) {
    int selected_number = select_number();
    send_num_to_leader(selected_number);

    int winner;
    if (rank == leader) {
      int num_to_guess = select_number();
      winner = compute_winner(num_to_guess);
    }
    send_winner(&winner);
    update_leader(winner);
    
    if (rank == 0) {
      update_leaderboard(winner);
      print_leaderboard(round, winner);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  free_vars();
  
  MPI_Finalize();
}
