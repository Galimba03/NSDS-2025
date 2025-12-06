#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>

/*
 * Group number: 09
 *
 * Group members
 * - Name Surname
 * - Name Surname
 */

const float L = 1000.0f;
const int num_particles = 10000;
const int num_iterations = 100;
const int print_every = 10;
const float max_velocity = 10.0f;

typedef struct {
    float x;
    float v;
} Particle;

// Generates a random float between min and max
float random_float(float min, float max) {
    return min + (float)rand() / ((float)RAND_MAX / (max - min));
}

// Helper to remove a particle from the array at index i (replaces with last element)
void remove_particle(Particle* particles, int* count, int index) {
    particles[index] = particles[*count - 1];
    (*count)--;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    srand(time(NULL) + rank);

    // Domain decomposition
    float sub_domain_len = L / size;
    float my_min_x = rank * sub_domain_len;
    float my_max_x = (rank + 1) * sub_domain_len;

    // Local storage (assume a safe max capacity to avoid complex reallocs during exam)
    int capacity = num_particles; 
    Particle* local_particles = (Particle*) malloc(capacity * sizeof(Particle));
    int local_count = 0;

    // Buffers for migrating particles
    Particle* outgoing_left = (Particle*) malloc(capacity * sizeof(Particle));
    Particle* outgoing_right = (Particle*) malloc(capacity * sizeof(Particle));

    // TODO: Initialization
    // Rank 0 generates particles and sends them to the correct owner.
    // Other ranks receive their initial particles.
    
    for (int iter = 0; iter < num_iterations; iter++) {
        int count_left = 0;
        int count_right = 0;

        // 1. Update Physics & Identify Migrating Particles
        for (int i = 0; i < local_count; i++) {
            local_particles[i].x += local_particles[i].v;

            // Bounce check (Global boundaries)
            if (local_particles[i].x < 0) {
                local_particles[i].x = -local_particles[i].x;
                local_particles[i].v = -local_particles[i].v;
            } else if (local_particles[i].x > L) {
                local_particles[i].x = 2 * L - local_particles[i].x;
                local_particles[i].v = -local_particles[i].v;
            }

            // Migration check
            // TODO: Logic to detect if particle leaves my_min_x or my_max_x
            // Move it to outgoing_left or outgoing_right buffers
            // Remove it from local_particles
        }

        // 2. Communication (Exchange Migrating Particles)
        // TODO: Send outgoing buffers to neighbors
        // TODO: Use MPI_Probe to detect incoming particles from neighbors
        // TODO: Receive particles and add them to local_particles


        // 3. Analysis
        if (iter % print_every == 0) {
            // TODO: Compute global max load (maximum number of particles on a single process)
            // Process 0 prints the result
        }
    }

    free(local_particles);
    free(outgoing_left);
    free(outgoing_right);

    MPI_Finalize();
    return 0;
}