#include "contiki.h"
#include "lib/random.h"
#include <stdio.h>

/* --- Shared Queue Configuration --- */
#define QUEUE_SIZE 5

static int buffer[QUEUE_SIZE];
static int head = 0;
static int tail = 0;
static int count = 0;

/* --- Custom Events for Synchronization --- */
static process_event_t event_queue_full;
static process_event_t event_queue_empty;

/* --- Processes Declaration --- */
PROCESS(producer_process, "Producer Process");
PROCESS(consumer_process, "Consumer Process");

AUTOSTART_PROCESSES(&producer_process, &consumer_process);

/*---------------------------------------------------------------------------*/
PROCESS_THREAD(producer_process, ev, data) {
	PROCESS_BEGIN();

	event_queue_full = process_alloc_event();
	event_queue_empty = process_alloc_event();

	while (1) {
		printf("Filling up the queue!\n");
		while (count < QUEUE_SIZE) {
			int item = random_rand() % 100;
			buffer[tail] = item;
			tail = (tail + 1) % QUEUE_SIZE;
			count++;

			printf("[PRODUCER] Added %d. Count: %d\n", item, count);
		}

		printf("[PRODUCER] Queue FULL. Signaling Consumer and waiting...\n");
		process_post(&consumer_process, event_queue_full, NULL);
		PROCESS_WAIT_EVENT_UNTIL(ev == event_queue_empty);
	}

	PROCESS_END();
}

/*---------------------------------------------------------------------------*/
/* Consumer Process */
PROCESS_THREAD(consumer_process, ev, data) {
	PROCESS_BEGIN();

	while (1) {
		if (count < QUEUE_SIZE) {
			printf("[CONSUMER] Waiting for queue to be FULL...\n");
			PROCESS_WAIT_EVENT_UNTIL(ev == event_queue_full);
		}

		printf("Emptying the queue!\n");
		while (count > 0) {
			int item = buffer[head];
			head = (head + 1) % QUEUE_SIZE;
			count--;

			printf("[CONSUMER] Got %d. Count: %d\n", item, count);
		}

		printf("[CONSUMER] Queue EMPTY. Signaling Producer and waiting...\n");
		process_post(&producer_process, event_queue_empty, NULL);
		PROCESS_WAIT_EVENT_UNTIL(ev == event_queue_full);
	}

	PROCESS_END();
}
/*---------------------------------------------------------------------------*/