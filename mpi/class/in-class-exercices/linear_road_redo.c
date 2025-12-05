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
  
  // 1. Calcola quanti segmenti possiede questo processo
  int segments_per_proc = num_segments / num_procs;

  // 2. Alloca due array: stato corrente e stato successivo (Double Buffering)
  int *current_road = (int*)calloc(segments_per_proc, sizeof(int));
  int *next_road = (int*)calloc(segments_per_proc, sizeof(int));

  // Variabili per la comunicazione
  int cars_exiting_local_road = 0; // Auto che escono dal mio ultimo segmento
  int cars_entering_local_road = 0; // Auto che arrivano dal processo precedente

  // Determina i vicini (Topologia Lineare)
  // Rank 0 riceve da nessuno (MPI_PROC_NULL), manda a 1
  // Rank N riceve da N-1, manda a nessuno (MPI_PROC_NULL -> le auto escono dalla simulazione)
  // TODO: ask this
  int source = (rank == 0) ? MPI_PROC_NULL : rank - 1;
  int dest   = (rank == num_procs - 1) ? MPI_PROC_NULL : rank + 1;

  for (int it = 0; it < num_iterations; ++it) {
    
    // A. Reset del buffer per il prossimo stato
    memset(next_road, 0, segments_per_proc * sizeof(int));
    cars_exiting_local_road = 0;

    // B. Logica di Movimento (Internal Processing)
    // Iteriamo su ogni segmento gestito da questo processo
    for (int i = 0; i < segments_per_proc; i++) {
        int num_cars = current_road[i];
        
        for (int c = 0; c < num_cars; c++) {
            if (move_next_segment()) {
                // L'auto si muove
                if (i == segments_per_proc - 1) {
                    // È nell'ultimo segmento locale -> va nel buffer di uscita
                    cars_exiting_local_road++;
                } else {
                    // Si sposta nel segmento successivo locale
                    next_road[i + 1]++;
                }
            } else {
                // L'auto sta ferma
                next_road[i]++;
            }
        }
    }

    // C. Comunicazione (Boundary Exchange)
    // Spedisco le auto che escono a destra, ricevo le auto che entrano da sinistra
    // MPI_Sendrecv gestisce automaticamente l'ordine per evitare deadlock
    // TODO: ask this
    MPI_Sendrecv(&cars_exiting_local_road, 1, MPI_INT, dest, 0,
                 &cars_entering_local_road, 1, MPI_INT, source, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // D. Gestione Ingressi (Segmento 0 locale)
    if (rank == 0) {
        // Se sono Rank 0, le auto arrivano dal generatore random
        cars_entering_local_road = create_random_input();
    }
    // Aggiungo le auto in ingresso (dal rank precedente o dal generatore) al primo segmento
    next_road[0] += cars_entering_local_road;

    // E. Swap dei puntatori (il "next" diventa "current" per il prossimo giro)
    int *temp = current_road;
    current_road = next_road;
    next_road = temp;

    // F. Calcolo Somma Globale (ogni 10 iterazioni)
    if (it % count_every == 0) {
      int local_sum = 0;
      for (int i = 0; i < segments_per_proc; i++) {
          local_sum += current_road[i];
      }

      int global_sum = 0;
      MPI_Reduce(&local_sum, &global_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
      
      if (rank == 0) {
        printf("Iteration: %d, sum: %d\n", it, global_sum);
      }
    }
  }

  free(current_road);
  free(next_road);
  
  // ATTENTION: The prof always put this here.
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}
