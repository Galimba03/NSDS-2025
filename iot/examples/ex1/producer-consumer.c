/*
 * Copyright (c) 2006, Swedish Institute of Computer Science.
 * All rights reserved.
 * ... (License header omitted) ...
 * This file is part of the Contiki operating system.
 */

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
static process_event_t event_data_ready;
static process_event_t event_slot_freed;

/* --- Processes Declaration --- */
/* this is the declaration of protothreads */
PROCESS(producer_process, "Producer Process");
PROCESS(consumer_process, "Consumer Process");

/* at the beginning of the problem we start manually both protothreads */
AUTOSTART_PROCESSES(&producer_process, &consumer_process);

/*---------------------------------------------------------------------------*/
/* Producer Process */
PROCESS_THREAD(producer_process, ev, data)
{
  PROCESS_BEGIN();

  static int iterations = 0;
  
  event_data_ready = process_alloc_event();
  event_slot_freed = process_alloc_event();

  while(1) {
    /* Critical constraint: Must wait if full */
    if(count >= QUEUE_SIZE) {
      /* IMPORTANTE: Se la coda è piena, facciamo una pausa forzata 
        * PRIMA di metterci in attesa dell'evento.
        * Questo pulisce la variabile 'ev' ed evita il bug del loop infinito
        * nel caso fossimo arrivati qui senza mai aver fatto pause.
        */
      PROCESS_PAUSE();
      PROCESS_WAIT_EVENT_UNTIL(ev == event_slot_freed);
    }

    /* Produce item */
    int item = random_rand() % 100;
    buffer[tail] = item;
    tail = (tail + 1) % QUEUE_SIZE;
    count++;
    
    printf("[PRODUCER] Added %d. Count: %d\n", item, count);
    iterations++;

    /* Notify consumer immediately */
    /* process_post is an asynchronous function */
    process_post(&consumer_process, event_data_ready, NULL);
    
    /* Invece di cedere sempre, cediamo solo il 50% delle volte.
     * Se NON entriamo nell'if, il while ricomincia subito e il Producer
     * aggiunge un altro elemento (creando un "burst").
     */
    if(random_rand() % 100 < 50) { 
        PROCESS_PAUSE();
    }
  }

  PROCESS_END();
}

/*---------------------------------------------------------------------------*/
/* Consumer Process */
PROCESS_THREAD(consumer_process, ev, data)
{
  PROCESS_BEGIN();

  static int iterations = 0;

  while(1) {
    /* Critical constraint: Must wait if empty */
    if(count == 0) {
        /* Pausa tattica per pulire 'ev' prima dell'attesa reale */
        PROCESS_PAUSE();
        PROCESS_WAIT_EVENT_UNTIL(ev == event_data_ready);
    }

    /* Consume item */
    int item = buffer[head];
    head = (head + 1) % QUEUE_SIZE;
    count--;

    printf("[CONSUMER] Removed %d. Count: %d\n", item, count);
    iterations++;

    /* Notify producer immediately */
    process_post(&producer_process, event_slot_freed, NULL);
    
    if(random_rand() % 100 < 50) {
        PROCESS_PAUSE();
    }
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/