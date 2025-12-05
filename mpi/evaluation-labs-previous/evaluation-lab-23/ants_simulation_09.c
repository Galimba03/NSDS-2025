#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>
#include <math.h>
#include <string.h>

const float min_val = 0;
const float max_val = 1000;
const int num_ants = 8 * 1000 * 1000;
const int num_food_sources = 10;
const int num_iterations = 500;

// Returns a random position between min_val and max_val
float random_position() {
  return (float) rand() / (float)(RAND_MAX/(max_val-min_val)) + min_val;
}

// Process 0 invokes this function to initialize food sources.
void init_food_sources(float* food_sources) {
  for (int i=0; i<num_food_sources; i++) {
    food_sources[i] = random_position();
  }
}

// Process 0 invokes this function to initialize the position of ants.
void init_ants(float* ants) {
  for (int i=0; i<num_ants; i++) {
    ants[i] = random_position();
  }
}

// Compute the average value for the positions array
float compute_average (float* positions, int size) {
  float sum = 0;
  for (int i = 0; i < size; i++) {
    sum += positions[i];
  }

  return sum / (float)size;
}

// Return the index of the nearest source of food for the ant_pos.
int index_of_nearest_source (float ant_pos, float *food_sources, int num_food_sources) {
  // Logic fix: must track index, not just value
  int nearest_idx = 0;
  float min_dist = fabs(food_sources[0] - ant_pos);

  for (int i = 1; i < num_food_sources; i++) {
    float dist = fabs(food_sources[i] - ant_pos);
    if (dist < min_dist) {
      min_dist = dist;
      nearest_idx = i;
    }
  }
  return nearest_idx;
}

// Update the position of the ant based on F1 and F2 forces
float update_position (float ant_pos, float center, float nearest_source) {
// FIX: Vector must be (Destination - Origin) to attract
  float d1 = nearest_source - ant_pos;
  float F1 = 0.01f * d1;

  // FIX: Vector must be (Destination - Origin) to attract
  float d2 = center - ant_pos;
  float F2 = 0.012f * d2;

  return (ant_pos + F1 + F2);
}

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);
    
  int rank;
  int num_procs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

  srand(time(NULL) + rank);

  const int ants_per_procs = num_ants / num_procs;

  // Pointers for data arrays
  float *global_ants = NULL;
  float *local_ants = NULL;
  float *food_sources = NULL;

  local_ants = (float*) malloc(ants_per_procs * sizeof(float));
  food_sources = (float*) malloc(num_food_sources * sizeof(float));
  
  // Process 0 initializes food sources and ants
  if (rank == 0) {
    global_ants = (float*) malloc(num_ants * sizeof(float));
    init_food_sources(food_sources);
    init_ants(global_ants);
  }

  MPI_Bcast(food_sources, num_food_sources, MPI_FLOAT, 0, MPI_COMM_WORLD);

  /*
    MPI_Scatter(
        global_ants,      // 1. Source Buffer (The Deck)
        ants_per_procs,   // 2. Send Count (Cards per person) -> if i put 1, I alternativly 1 element to each process existing.
        MPI_FLOAT,        // 3. Send Type
        local_ants,       // 4. Destination Buffer (Your Hand)
        ants_per_procs,   // 5. Receive Count (Cards you expect)
        MPI_FLOAT,        // 6. Receive Type
        0,                // 7. Root (The Dealer)
        MPI_COMM_WORLD    // 8. Communicator
    );
  */
  MPI_Scatter(global_ants, ants_per_procs, MPI_FLOAT, 
              local_ants, ants_per_procs, MPI_FLOAT, 
              0, MPI_COMM_WORLD);
  
  float local_avg = compute_average(local_ants, ants_per_procs);

  float global_avg = 0;
  MPI_Allreduce(&local_avg, &global_avg, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);

  float center = global_avg / num_procs;
  if (rank == 0) {
    printf("Initial Center: %f\n", center);
  }

  for (int iter = 0; iter < num_iterations; iter++) {
    // Iterate through local_ants, find nearest source, use update_position()
    for (int ant = 0; ant < ants_per_procs; ant++) {
      int idx = index_of_nearest_source(local_ants[ant], food_sources, num_food_sources);
      local_ants[ant] = update_position(local_ants[ant], center, food_sources[idx]);
    }

    // 1. Calculate local sum/average
    local_avg = compute_average(local_ants, ants_per_procs);
    // 2. Use MPI to compute the global center (Reduce?)
    MPI_Allreduce(&local_avg, &global_avg, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
    // 3. Make sure everyone knows the new center for the next iteration
    center = global_avg / num_procs;

    if (rank == 0) {
      printf("Iteration: %d - Average position: %f\n", iter, center);
    }
  }

  if (rank == 0) {
    free(global_ants);
  }
  free(local_ants);
  free(food_sources);
  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}