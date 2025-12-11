#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

// Group number: 09
// Group members:
// Luca Komisarjevsky - 10861704
// Davide Ghisolfi - 10839162
// Matteo Galimberti - 10819040

const int N = 12;

// Returns the value at the given row and column
int val(int *A, int r, int c) {
  return A[r * N + c];
}

// Allocates and initializes matrix
int* generate_matrix() {
  int* A = (int*) malloc(N * N * sizeof(int));
  for (int i = 0; i < N * N; i++) {
    A[i] = rand() % 100;
  }

  for (int i = 0; i < N; i++) {
    for (int j = 0; j < N; j++) {
      printf("%d ", val(A, i, j));
    }
    printf("\n");
  }

  return A;
}

// Check if center is local minima
// 0 = false, 1 = true
int is_local_minima(int center, int up, int right, int down, int left) {
  return ((center < up) && (center < right) && (center < down) && (center < left));
}

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  // We modified "size" just to simplify
  int rank, num_proc;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_proc);

  if (N % num_proc != 0) {
    if (rank == 0) 
      printf("N must be a multiple of the number of processes.\n");
    MPI_Finalize();
    return 0;
  }

  int num_row_per_proc = N / num_proc;
  int elements_for_proc = num_row_per_proc * N;

  // ------------------------------------------------------------
  // Generate matrix on rank 0
  // ------------------------------------------------------------
  int* fullA = NULL;

  if (rank == 0) {
    fullA = generate_matrix();
  }

  
  // ------------------------------------------------------------
  // Distribute to all processes
  // ------------------------------------------------------------
  int* local = (int*) malloc(elements_for_proc * sizeof(int));

  MPI_Scatter(fullA, elements_for_proc, MPI_INT, local, elements_for_proc, MPI_INT, 0, MPI_COMM_WORLD);
  // Now everyone have the local matrix that need to operate on ready

  // ------------------------------------------------------------
  // Free global matrix
  // ------------------------------------------------------------
  if (rank == 0) {
    free(fullA);
  }
  
  // ------------------------------------------------------------
  // Exchange futher information if needed
  // ------------------------------------------------------------
  
  // next_matrix_first_line is the line of elements that are below the actual process' matrix
  int* next_matrix_first_line = (int*) malloc(N * sizeof(int));
  // previous_matrix_last_line is the line of elements that are above the actual process' matrix
  int* previous_matrix_last_line = (int*) malloc(N * sizeof(int));

  if (rank > 0) {
    MPI_Sendrecv(local, N, MPI_INT, (rank-1), 0, previous_matrix_last_line, N, MPI_INT, (rank-1), 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }
  if (rank < (num_proc - 1)) {
    MPI_Sendrecv((local+elements_for_proc-N), N, MPI_INT, (rank+1), 0, next_matrix_first_line, N, MPI_INT, (rank+1), 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  // For generalization, we set next and previous arrays to first line of matrix and last respectivly
  if (rank == 0) {
    for (int i = 0; i < N; i++) {
      previous_matrix_last_line[i] = val(local, 0, i);
    }
  }
  if (rank == (num_proc-1)) {
    for (int i = 0; i < N; i++) {
      next_matrix_first_line[i] = val(local, num_row_per_proc-1, i);
    }
  }

  // ------------------------------------------------------------
  // Compute local minima (excluding GLOBAL borders)
  // ------------------------------------------------------------
  int initial_row = 0;
  int final_row = num_row_per_proc;
  if (rank == 0) {
    initial_row += 1;
  }
  if (rank == (num_proc - 1)) {
    final_row -= 1;
  }

  int* counter_row = (int*) calloc(num_row_per_proc, sizeof(int));

  for (int i = initial_row; i < final_row; i++) {
    for (int j = 1; j < (N-1); j++) {
      int up = 0;
      int down = 0;

      if (i == initial_row) {
        up = previous_matrix_last_line[j];
      } else {
        up = val(local, i-1, j);
      }
      if (i == (final_row - 1)) {
        down = next_matrix_first_line[j];
      } else {
        down = val(local, i+1, j);
      }
      int center = val(local, i, j);
      int left = val(local, i, j-1);
      int right = val(local, i, j+1);

      counter_row[i] += is_local_minima(center, up, right, down, left);
    }
  }

  // ------------------------------------------------------------
  // Send results to rank 0 and print results on rank 0
  // ------------------------------------------------------------
  int* global_counter_row = NULL;

  if(rank == 0) {
    global_counter_row = (int*) malloc(N * sizeof(int));
  }


  MPI_Gather(counter_row, num_row_per_proc, MPI_INT, global_counter_row, num_row_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    for (int i = 0; i < N; i++) {
      printf("Row: %d, local minima: %d\n", i, global_counter_row[i]);
    }
  }
  
  // ------------------------------------------------------------
  // Free allocated memory
  // ------------------------------------------------------------

  free(local);
  free(previous_matrix_last_line);
  free(next_matrix_first_line);
  free(global_counter_row);
  free(counter_row);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
