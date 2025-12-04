#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> // For sleep() to simulate work

#define TAG_DATA 0
#define NUM_ELEMENTS 1000

// Helper to print cleanly
void log_msg(int rank, const char* msg) {
    printf("[Rank %d]: %s\n", rank, msg);
}

/**
 * SCENARIO 1: Standard Blocking Point-to-Point
 * --------------------------------------------
 * MPI_Send blocks until the buffer is safe to reuse.
 * MPI_Recv blocks until the data is fully received.
 */
void demo_blocking_p2p(int rank) {
    int data;
    if (rank == 0) {
        data = 42;
        log_msg(rank, "Sending 42 to Rank 1...");
        MPI_Send(&data, 1, MPI_INT, 1, TAG_DATA, MPI_COMM_WORLD);
    } else if (rank == 1) {
        MPI_Recv(&data, 1, MPI_INT, 0, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("[Rank %d]: Received data %d\n", rank, data);
    }
}

/**
 * SCENARIO 2: Deadlock & The Fix (MPI_Sendrecv)
 * ---------------------------------------------
 * DEADLOCK EXPLANATION:
 * If both ranks try to Receive before Sending (or both Send huge data 
 * that doesn't fit in the system buffer before Receiving), they will wait 
 * for each other forever.
 */
void demo_deadlock_fix(int rank, int num_procs) {
    if (num_procs < 2) return;

    int send_buf = rank;
    int recv_buf;

    // --- 2A. THE WRONG WAY (Potential Deadlock) ---
    /*
    // If we uncomment this, and the message size is large, 
    // both processes block here waiting for the send to complete, 
    // but neither can receive.
    
    MPI_Ssend(&send_buf, 1, MPI_INT, 1 - rank, TAG_DATA, MPI_COMM_WORLD); // Ssend forces sync
    MPI_Recv(&recv_buf, 1, MPI_INT, 1 - rank, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    */

    // --- 2B. THE SAFE WAY (MPI_Sendrecv) ---
    // This function intelligently organizes the send and receive to avoid blocking.
    // Use this for "Shift" or "Exchange" patterns.
    
    int neighbor = (rank == 0) ? 1 : 0; // Simple swap between 0 and 1
    
    log_msg(rank, "Exchanging data safely...");
    
    MPI_Sendrecv(&send_buf, 1, MPI_INT, neighbor, TAG_DATA,
                 &recv_buf, 1, MPI_INT, neighbor, TAG_DATA,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    printf("[Rank %d]: Sent %d, Received %d\n", rank, send_buf, recv_buf);
}

/**
 * SCENARIO 3: Non-Blocking (Asynchronous)
 * ---------------------------------------
 * Allows overlapping computation with communication.
 * Key Functions: MPI_Isend, MPI_Irecv, MPI_Wait
 */
/**
 * MPI_Isend (Non Bloccante / Immediate)
    Quando chiami MPI_Isend, la funzione ritorna istantaneamente, quasi a tempo zero. Il programma passa subito alla riga successiva.
    Cosa succede: Hai solo detto a MPI "Per favore, inizia a spedire questi dati in background".
    Il Pericolo: Dato che la funzione ritorna subito, l'invio potrebbe non essere ancora iniziato o finito. NON puoi toccare o modificare la variabile che hai inviato finché non verifichi che l'invio è completo. Se modifichi la variabile subito dopo MPI_Isend, potresti inviare dati corrotti (metà vecchi, metà nuovi).
    La Verifica: Devi usare MPI_Wait (o MPI_Test) per aspettare che l'operazione in background finisca, prima di poter riutilizzare quella variabile.

    MPI_Request request;
    buffer[0] = 100;

    // Ritorna SUBITO. L'invio avviene in background.
    MPI_Isend(buffer, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, &request);

    // !!! PERICOLO - Non toccare 'buffer' qui !!!
    // Ma puoi fare calcoli che NON usano 'buffer'
    fai_calcoli_complessi(); // <--- Questo avviene MENTRE i dati viaggiano!

    // Ora aspetto che l'invio sia finito prima di riusare il buffer
    MPI_Wait(&request, MPI_STATUS_IGNORE);

    // Ora è sicuro modificare il buffer
    buffer[0] = 200;
 */
void demo_non_blocking(int rank) {
    if (rank > 1) return; // Demo for first 2 ranks only

    int buffer[NUM_ELEMENTS];
    MPI_Request request;

    if (rank == 0) {
        // Fill buffer
        for(int i=0; i<NUM_ELEMENTS; i++) buffer[i] = i;

        log_msg(rank, "Initiating Non-Blocking Send...");
        // This returns IMMEDIATELY, does not wait for data to leave
        MPI_Isend(buffer, NUM_ELEMENTS, MPI_INT, 1, TAG_DATA, MPI_COMM_WORLD, &request);

        // DO SOME WORK HERE while data is flying (Overlapping)
        log_msg(rank, "Doing computation while message travels...");
        sleep(1); 

        // Now wait to ensure buffer is free to reuse
        MPI_Wait(&request, MPI_STATUS_IGNORE);
        log_msg(rank, "Send Complete.");
    } 
    else if (rank == 1) {
        log_msg(rank, "Initiating Non-Blocking Recv...");
        // Returns IMMEDIATELY
        MPI_Irecv(buffer, NUM_ELEMENTS, MPI_INT, 0, TAG_DATA, MPI_COMM_WORLD, &request);

        log_msg(rank, "Doing other work...");
        sleep(1);

        // Wait until data actually arrives
        MPI_Wait(&request, MPI_STATUS_IGNORE);
        printf("[Rank %d]: Data arrived. First element: %d\n", rank, buffer[0]);
    }
}

/**
 * SCENARIO 4: Collective Communication
 * ------------------------------------
 * Scatter, Gather, Reduce, Broadcast.
 * Note: All processes must call these.
 */
void demo_collectives(int rank, int num_procs) {
    int root = 0;
    
    // --- 4A. Broadcast (One to All) ---
    int magic_number = (rank == root) ? 999 : 0;
    MPI_Bcast(&magic_number, 1, MPI_INT,
                root, MPI_COMM_WORLD);
    // Everyone now has 999
    
    // --- 4B. Scatter (Splitting an array) ---
    int *global_data = NULL;
    int local_data; 

    if (rank == root) {
        global_data = (int*)malloc(num_procs * sizeof(int));
        for (int i=0; i<num_procs; i++) {
            global_data[i] = i * 10;
        }
        log_msg(rank, "Scattering array [0, 10, 20...]");
    }

    // Sends one integer to each process
    MPI_Scatter(global_data, 1, MPI_INT, 
                &local_data, 1, MPI_INT, 
                root, MPI_COMM_WORLD);
    
    // Modify local data
    local_data += 5; 

    // --- 4C. Gather (Collecting results) ---
    // Collects all local_data back into global_data at root
    MPI_Gather(&local_data, 1, MPI_INT, 
                global_data, 1, MPI_INT, 
                root, MPI_COMM_WORLD);

    if (rank == root) {
        printf("[Rank 0]: Gathered result from P1: %d (Expected 15)\n", global_data[1]);
        free(global_data);
    }
}

/**
 * SCENARIO 5: Probing (Unknown Message Size)
 * ------------------------------------------
 * checking a message before receiving it.
 */
void demo_probe(int rank) {
    if (rank == 0) {
        // Send a random amount of numbers (between 1 and 100)
        int count = 50;
        int *buf = (int*)malloc(count * sizeof(int));
        MPI_Send(buf, count, MPI_INT, 
                    1, TAG_DATA, MPI_COMM_WORLD);
        free(buf);
    } else if (rank == 1) {
        MPI_Status status;
        // 1. Peek at the message without removing it
        MPI_Probe(0, TAG_DATA, MPI_COMM_WORLD, &status);

        // 2. Find out how big it is
        int count;
        MPI_Get_count(&status, MPI_INT, &count);

        printf("[Rank 1]: Probed message size is %d. Allocating memory...\n", count);

        // 3. Allocate and Recv
        int *recv_buf = (int*)malloc(count * sizeof(int));
        MPI_Recv(recv_buf, count, MPI_INT, 0, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        free(recv_buf);
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int rank, num_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if (num_procs < 2) {
        if (rank == 0) printf("Please run with at least 2 processes.\n");
        MPI_Finalize();
        return 0;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) printf("\n=== DEMO 1: BLOCKING P2P ===\n");
    demo_blocking_p2p(rank);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) printf("\n=== DEMO 2: DEADLOCK FIX (Sendrecv) ===\n");
    demo_deadlock_fix(rank, num_procs);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) printf("\n=== DEMO 3: NON-BLOCKING (Isend/Irecv) ===\n");
    demo_non_blocking(rank);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) printf("\n=== DEMO 4: COLLECTIVES ===\n");
    demo_collectives(rank, num_procs);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) printf("\n=== DEMO 5: PROBE ===\n");
    demo_probe(rank);
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();
    return 0;
}