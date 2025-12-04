#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/**
 * Group number: 09
 *
 * Group members
 * Galimberti Matteo
 * Komisarjevksy Luca
 * Ghisolfi Davide
 *
 **/

// Set DEBUG 1 if you want car movement to be deterministic
#define DEBUG 0

const int num_segments = 256;

const int num_iterations = 1000;
const int count_every = 10;

const double alpha = 0.5;
const int max_in_per_sec = 10;

// Returns the number of car that enter the first segment at a given iteration.
int create_random_input() {
  #if DEBUG
  return 1;
  #else

  return rand() % max_in_per_sec;
#endif
}

// Returns 1 if a car needs to move to the next segment at a given iteration, 0 otherwise.
int move_next_segment() {
  #if DEBUG
  return 1;
  #else
  return rand() < alpha ? 1 : 0;
#endif
}

int main(int argc, char** argv) { 
  MPI_Init(NULL, NULL);

  int rank;
  int num_procs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  srand(time(NULL) + rank);
  
  /*
    Il testo dice: "Consider a linear road divided into consecutive segments... assume number of segments to be a multiple of the number of processes". Nel tuo codice, ogni processo gestisce un solo "numero" (local_cars), trattando l'intero processo come se fosse un unico segmento. Invece, se num_segments = 256 e hai 4 processi, ogni processo deve gestire un array di 64 segmenti.

    Ecco le correzioni principali necessarie:
      - Array Locale: Devi allocare un array (la strada locale) di dimensione num_segments / num_procs.

      - Double Buffering: Quando muovi le auto, non puoi aggiornare l'array "in-place" (nello stesso array) mentre lo stai leggendo, altrimenti rischi di muovere la stessa auto più volte nello stesso turno. È meglio usare due array (current_road e next_road).

      - Comunicazione: MPI_Isend usato in quel modo è pericoloso perché modifichi il buffer outgoing_cars nel ciclo successivo potenzialmente prima che la send sia finita. MPI_Sendrecv è molto più sicuro e pulito per questi pattern a catena (shift).
  */
  // define and init variables
  MPI_Request request; 
  int local_cars = 0;
  int outgoing_cars = 0;
  int ingoing_cars = 0;
  
  // Simulate for num_iterations iterations
  for (int it = 0; it < num_iterations; ++it) {
    // Move cars across segments
    if (rank != (num_procs-1)) {

      for (int i = 0; i < local_cars; i++) {
        if (move_next_segment()) {
          local_cars--;
          outgoing_cars++;
        }
      }

      MPI_Isend(&outgoing_cars, 1, MPI_INT, rank+1, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
    }

    if (rank > 0) {
      MPI_Recv(&ingoing_cars, 1, MPI_INT, rank-1, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    } 
    // New cars may enter in the first segment
    else {
      ingoing_cars = create_random_input();
    }
    local_cars += ingoing_cars;

    // Cars may exit from the last segment
    if (rank == (num_procs-1)) {
      for (int i = 0; i < local_cars; i++) {
        if (move_next_segment()) {
          local_cars--;
          outgoing_cars++;
        }
      }
    }

    // When needed, compute the overall sum
    if (it%count_every == 0) {
      int global_sum = 0;

      // compute global sum
      MPI_Reduce(&local_cars, &global_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
      
      if (rank == 0) {
	      printf("Iteration: %d, sum: %d\n", it, global_sum);
      }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
  }

  // deallocate dynamic variables, if needed
  
  MPI_Finalize();
}
