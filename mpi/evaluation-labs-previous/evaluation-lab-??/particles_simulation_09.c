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
const int num_iterations = 10000;
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
    Particle* local_particles;
    int local_count = 0;

    // Buffers for migrating particles
    Particle* outgoing_left = (Particle*) malloc(capacity * sizeof(Particle));
    Particle* outgoing_right = (Particle*) malloc(capacity * sizeof(Particle));

    // --- INITIALIZATION PHASE ---
    if (rank == 0) {
        // Rank 0 is the Generator
        
        // 1. Create temporary buckets for each process
        // counts keeps track of how many elements have been added to the array that will go to process with rank i
        int *counts = (int*) calloc(size, sizeof(int));
        // buckets is an array of pointer of dimention of number of processes that points to the array of elements that will go to process i
        Particle **buckets = (Particle**) malloc(size * sizeof(Particle*));
        for(int i=0; i<size; i++) {
            buckets[i] = (Particle*) malloc(num_particles * sizeof(Particle));
        }

        // 2. Generate and Sort
        for (int i = 0; i < num_particles; i++) {
            float x = random_float(0, L);
            float v = random_float(-1, 1);
            
            // Calculate who owns this particle (0..size-1)
            int owner = (int)(x / sub_domain_len);
            if (owner >= size) {
                owner = size - 1; // Handle edge case x=L
            }

            // Add particle to the correct bucket
            buckets[owner][counts[owner]].x = x;
            buckets[owner][counts[owner]].v = v;
            counts[owner]++;
        }

        // 3. Distribute
        // Keep Rank 0's data for itself
        local_count = counts[0];
        local_particles = (Particle*) malloc(local_count * sizeof(Particle));
        // Copy buckets[0] into local_particles
        for(int i = 0; i < local_count; i++) {
            local_particles[i] = buckets[0][i];
        }

        // Send to others
        for (int r = 1; r < size; r++) {
            // Send buckets[r] to rank 'r' using MPI_BYTE to avoid struct type definition
            MPI_Send(buckets[r], counts[r] * sizeof(Particle), MPI_BYTE, r, 0, MPI_COMM_WORLD);
        }

        // Cleanup temporary buckets
        for(int i=0; i<size; i++) {
            free(buckets[i]);
        }
        free(buckets);
        free(counts);

    } else {
        // --- WORKER NODES ---
        // Use MPI_Probe to check incoming message from Rank 0
        MPI_Status status;
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);

        // Get size of the message in bytes
        int number_of_bytes;
        MPI_Get_count(&status, MPI_BYTE, &number_of_bytes);
    
        // Allocate full capacity (e.g., 10000) to ensure room for future migrations
        local_particles = (Particle*) malloc(capacity * sizeof(Particle));

        // Receive the data
        MPI_Recv(local_particles, number_of_bytes, MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    
    // --- SIMULATION LOOP ---
    for (int iter = 0; iter < num_iterations; iter++) {
        int count_left = 0;
        int count_right = 0;

        // 1. Update Physics & Identify Migrating Particles
        for (int i = 0; i < local_count; i++) {
            local_particles[i].x += local_particles[i].v;

            // Bounce check (Global boundaries)
            // Left Bounce (Rank 0 only)
            if (rank == 0 && local_particles[i].x < 0) {
                local_particles[i].x = -local_particles[i].x;
                local_particles[i].v = -local_particles[i].v;
            } 
            // Right Bounce (Rank N-1 only)
            else if (rank == (size-1) && local_particles[i].x > L) {
                local_particles[i].x = 2.0f * L - local_particles[i].x;
                local_particles[i].v = -local_particles[i].v;
            }

            // Migration check
            int migrated = 0;
            
            // Check Right (only if not last rank)
            if (rank != size - 1 && local_particles[i].x >= my_max_x) {
                outgoing_right[count_right++] = local_particles[i];
                migrated = 1;
            } 
            // Check Left (only if not first rank)
            else if (rank != 0 && local_particles[i].x < my_min_x) {
                outgoing_left[count_left++] = local_particles[i];
                migrated = 1;
            }

            // Removal Logic (Swap with Last)
            if (migrated) {
                local_particles[i] = local_particles[local_count - 1];
                local_count--;
                
                i--;
            }
        }

        // 2. Communication
        // Use Isend to prevent deadlock
        MPI_Request reqs[2];
        int num_reqs = 0;

        if (rank != 0) {
             MPI_Isend(outgoing_left, count_left * sizeof(Particle), MPI_BYTE, rank - 1, 0, MPI_COMM_WORLD, &reqs[num_reqs++]);
        }
        if (rank != size - 1) {
             MPI_Isend(outgoing_right, count_right * sizeof(Particle), MPI_BYTE, rank + 1, 0, MPI_COMM_WORLD, &reqs[num_reqs++]);
        }
        
        // Receive from Left Neighbor (if exists)
        if (rank != 0) {
            MPI_Status status;
            // Check specifically for message from rank-1
            MPI_Probe(rank - 1, 0, MPI_COMM_WORLD, &status);

            int bytes;
            MPI_Get_count(&status, MPI_BYTE, &bytes);
            int count_in = bytes / sizeof(Particle);

            // Receive directly into the tail of the array
            // &local_particles[local_count] is the address of the first free slot
            MPI_Recv(local_particles + local_count, bytes, MPI_BYTE, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            // Update the count
            local_count += count_in;
        }

        // Receive from Right Neighbor (if exists)
        if (rank != size - 1) {
            MPI_Status status;
            MPI_Probe(rank + 1, 0, MPI_COMM_WORLD, &status);

            int bytes;
            MPI_Get_count(&status, MPI_BYTE, &bytes);
            int count_in = bytes / sizeof(Particle);

            // Append to the tail
            MPI_Recv(local_particles + local_count, bytes, MPI_BYTE, rank + 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            local_count += count_in;
        }

        // Ensure non-blocking sends are complete before reusing outgoing buffers
        MPI_Waitall(num_reqs, reqs, MPI_STATUSES_IGNORE);

        // 3. Analysis
        if (iter % print_every == 0) {
            int global_max_load = 0;
            MPI_Reduce(&local_count, &global_max_load, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
            
            if (rank == 0) {
                printf("Iter %d: Max particles on a node: %d\n", iter, global_max_load);
            }
        }
    }

    free(local_particles);
    free(outgoing_left);
    free(outgoing_right);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}