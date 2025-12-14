#include "contiki.h"
#include "sys/etimer.h"
#include "sys/ctimer.h"
#include "sys/timer.h"
#include "sys/stimer.h"
#include "sys/rtimer.h"
#include <stdio.h>

/* --- CONFIGURATION --- */
/* Rtimer needs a hard interval (hardware ticks) */
#define RTIMER_INTERVAL (RTIMER_SECOND / 2) /* 0.5 seconds */
#define POLLING_INTERVAL (CLOCK_SECOND)     /* 1 second */

/* --- VARIABLES --- */
static struct etimer et_periodic; // Event timer (Main loop)
static struct ctimer ct_callback; // Callback timer
static struct timer  t_passive;   // Passive timer (ticks)
static struct stimer st_passive;  // Passive stimer (seconds)
static struct rtimer rt_task;     // Real-time task

/* --- CALLBACKS --- */

/* CTIMER CALLBACK: Runs in process context, cooperative */
static void ctimer_func(void *ptr)
{
  printf("[CTIMER] Callback executed. Rescheduling...\n");
  /* Reset ensures stable period avoiding drift */
  ctimer_reset(&ct_callback);
}

/* RTIMER CALLBACK: Runs in INTERRUPT context (Preemptive!) */
void rtimer_func(struct rtimer *t, void *ptr)
{
  /* CAUTION: Printf in rtimer is risky on real HW (too slow for ISR).
   * Used here only for demo in Cooja/MSPSim.
   */
  printf("[RTIMER] Hard Interrupt fired at %lu\n", (unsigned long)RTIMER_NOW());

  /* Reschedule manually for precise periodic execution */
  rtimer_set(&rt_task, RTIMER_NOW() + RTIMER_INTERVAL, 1,
             (rtimer_callback_t)rtimer_func, NULL);
}

/* --- MAIN PROCESS --- */
PROCESS(timers_demo_process, "All Timers Demo");
AUTOSTART_PROCESSES(&timers_demo_process);

PROCESS_THREAD(timers_demo_process, ev, data)
{
  PROCESS_BEGIN();

  printf("--- Starting Contiki-NG Timers Demo ---\n");

  /* 1. SETUP PASSIVE TIMERS (timer & stimer) 
   * They don't generate events, we must poll them.
   */
  timer_set(&t_passive, 3 * CLOCK_SECOND);    // Expires in 3s
  stimer_set(&st_passive, 10);                // Expires in 10s

  /* 2. SETUP CTIMER 
   * Will call ctimer_func after 2 seconds
   */
  ctimer_set(&ct_callback, 2 * CLOCK_SECOND, ctimer_func, NULL);

  /* 3. SETUP RTIMER 
   * Will interrupt everything 0.5s from now
   */
  rtimer_set(&rt_task, RTIMER_NOW() + RTIMER_INTERVAL, 1,
             (rtimer_callback_t)rtimer_func, NULL);

  /* 4. SETUP ETIMER (The heartbeat)
   * Wakes up this process every 1 second
   */
  etimer_set(&et_periodic, POLLING_INTERVAL);

  while(1) {
    /* Wait for the Etimer (or other events) */
    PROCESS_WAIT_EVENT_UNTIL(ev == PROCESS_EVENT_TIMER);

    if(etimer_expired(&et_periodic)) {
      printf("[ETIMER] Process woke up (1s tick).\n");

      /* Check Passive Timer (Manual Polling) */
      if(timer_expired(&t_passive)) {
        printf("[TIMER]  The 3-second passive timer expired!\n");
        timer_restart(&t_passive); /* Restart from now (drifts allowed) */
      } else {
        printf("[TIMER]  Pending... Remaining: %lu ticks\n", 
               (unsigned long)timer_remaining(&t_passive));
      }

      /* Check Passive Stimer (Seconds) */
      if(stimer_expired(&st_passive)) {
        printf("[STIMER] The 10-second timer expired!\n");
        stimer_reset(&st_passive);
      }

      /* Keep the heartbeat alive */
      etimer_reset(&et_periodic);
    }
  }

  PROCESS_END();
}