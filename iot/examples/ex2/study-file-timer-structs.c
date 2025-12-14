/**
 * \file
 * Tutorial completo sui Timer di Contiki-NG
 * Spiegazione delle struct e della sincronizzazione drift-free.
 * \author
 * Tuo Nome (con l'aiuto di Gemini)
 */

#include "contiki.h"
#include "sys/etimer.h"  /* Event Timer (Processi) */
#include "sys/ctimer.h"  /* Callback Timer (Cooperative) */
#include "sys/rtimer.h"  /* Real-time Timer (Preemptive/ISR) */
#include "sys/timer.h"   /* Passive Timer (Polling) */
#include <stdio.h>

/* --- CONFIGURAZIONE INTERVALLI --- */

/* * CLOCK_SECOND: Tick del sistema operativo (es. 128 Hz su Sky).
 * Usato da: etimer, ctimer, timer.
 */
#define SW_INTERVAL (2 * CLOCK_SECOND)  /* 2 Secondi */

/* * RTIMER_SECOND: Tick dell'hardware (es. 32768 Hz su Sky).
 * Usato da: rtimer. Ha una risoluzione molto più alta.
 */
#define HW_INTERVAL (RTIMER_SECOND / 2) /* 0.5 Secondi */

/* --- DICHIARAZIONE DELLE STRUCT --- */
/** NOTA SULLE STRUCT (Cosa c'è dentro?):
 *
 * struct timer {
 * 	clock_time_t start;     // Quando è stato settato il timer (tempo sistema)
 * 	clock_time_t interval;  // Quanto deve durare l'attesa
 * };
 * 
 * 1. static struct etimer et;
 * Contiene: 
 * - struct timer (start + interval)
 * - struct process *p (chi svegliare)
 * - struct etimer *next (lista linkata)
 * 
 * struct etimer {
 * 	struct timer timer;  // <--- Contiene la struct timer vista sopra!
 * 	struct process *p;   // Puntatore al processo da svegliare
 * 	struct etimer *next; // Puntatore al prossimo etimer (lista collegata)
 * };
 * 
 * 2. static struct rtimer rt;
 * Contiene:
 * - rtimer_clock_t time (IL MOMENTO ESATTO DELLO SCATTO)
 * - rtimer_callback_t func (La funzione da chiamare)
 * 
 * struct rtimer {
 * 	rtimer_clock_t time;      // IL TEMPO ESATTO IN CUI DEVE SCATTARE (Assoluto)
 * 	rtimer_callback_t func;   // La funzione da chiamare (puntatore a funzione)
 * 	void *ptr;                // I dati da passare alla funzione
 * };
 *
 * 3. static struct ctimer ct;
 * Contiene:
 * - struct etimer et (usa un etimer sotto il cofano)
 * - void (*f)(void *) (puntatore alla funzione)
 */

static struct etimer et_periodic;
static struct ctimer ct_callback;
static struct rtimer rt_hard;
static struct timer  t_passive;

/* --- CALLBACKS --- */

/** CTIMER CALLBACK (Cooperative)
 * Questa funzione viene chiamata dal sistema operativo quando ha tempo.
 * Non interrompe altre operazioni critiche.
 */
void ctimer_action(void *ptr)
{
  printf("[CTIMER] Soft-timer scaduto (Tick OS: %lu)\n", clock_time());

  /* * TRUCCO PER LA PRECISIONE (Drift-Free):
   * ctimer_reset vs ctimer_restart
   * * _restart(&ct): Calcola la prossima scadenza partendo da ADESSO.
   * Se il sistema è in ritardo, il ritardo si accumula.
   *
   * _reset(&ct):   Calcola la prossima scadenza partendo da QUANDO DOVEVA SCADERE.
   * Mantiene il periodo costante recuperando il ritardo.
   */
  ctimer_reset(&ct_callback);
}

/** RTIMER CALLBACK (Preemptive / Hard Real-Time)
 * Questa funzione viene eseguita in contesto di INTERRUPT.
 * Blocca tutto il resto. Deve essere velocissima.
 * (Printf qui è pericoloso su hardware vero, ma ok in Cooja).
 */
void rtimer_action(struct rtimer *t, void *ptr)
{
  /** ANALISI DELLA STRUCT RTIMER:
   * 't' è il puntatore alla struct rtimer rt_hard definita sopra.
   * t->time contiene il valore del clock quando questo timer ERA PROGRAMMATO.
   * RTIMER_NOW() contiene il valore del clock ADESSO.
   */
  printf("[RTIMER] Interrupt Hardware! (Tick HW: %lu | Now time: %lu)\n", (unsigned long)t->time, (unsigned long)RTIMER_NOW());

  /* * CALCOLO DEL PROSSIMO SLOT (Drift-Free)
   * Non usiamo RTIMER_NOW() + INTERVALLO.
   * Usiamo t->time (la vecchia scadenza ideale) + INTERVALLO.
   * Così se siamo in ritardo di 5 tick, accorciamo la prossima attesa di 5 tick.
   */
  rtimer_clock_t next_slot = t->time + HW_INTERVAL;

  /* Riprogramma il timer */
  int ret = rtimer_set(t, next_slot, 1, (rtimer_callback_t)rtimer_action, ptr);
  
  if(ret != RTIMER_OK) {
    printf("[RTIMER] Errore: siamo troppo in ritardo per schedulare il prossimo!\n");
  }
}

/* --- PROCESSO PRINCIPALE --- */

PROCESS(demo_timers_process, "Demo Timers Process");
AUTOSTART_PROCESSES(&demo_timers_process);

PROCESS_THREAD(demo_timers_process, ev, data)
{
  PROCESS_BEGIN();

  printf("--- Inizio Tutorial Timers ---\n");
  printf("SW Interval: %lu ticks | HW Interval: %u ticks\n", 
         (unsigned long)SW_INTERVAL, (unsigned int)HW_INTERVAL);

  /* 1. Avvio RTIMER (Immediato + Offset) */
  rtimer_set(&rt_hard, RTIMER_NOW() + HW_INTERVAL, 1, (rtimer_callback_t)rtimer_action, NULL);

  /* 2. Avvio CTIMER (Callback) */
  ctimer_set(&ct_callback, SW_INTERVAL, ctimer_action, NULL);

  /* 3. Avvio ETIMER (Loop Principale) */
  etimer_set(&et_periodic, CLOCK_SECOND); /* Ogni secondo */

  /* 4. Avvio TIMER PASSIVO (Solo per polling) */
  timer_set(&t_passive, 5 * CLOCK_SECOND);

  while(1) {
    /* * YIELD: Il processo si ferma qui finché non scade l'etimer 
     * (o arriva un altro evento).
     */
    PROCESS_WAIT_EVENT_UNTIL(ev == PROCESS_EVENT_TIMER);

    /* Verifichiamo chi ha generato l'evento (potrebbe non essere et_periodic) */
    if(etimer_expired(&et_periodic)) {
      
      printf("[MAIN LOOP] Sveglia periodica (1s).\n");

      /* Esempio di uso del TIMER PASSIVO */
      if(timer_expired(&t_passive)) {
        printf(" -> [PASSIVE] Sono passati 5 secondi! Resetto.\n");
        timer_restart(&t_passive); /* Restart usa "adesso" come base */
      } else {
        printf(" -> [PASSIVE] Mancano %lu tick.\n", (unsigned long)timer_remaining(&t_passive));
      }

      /* Resetto l'etimer per il prossimo secondo */
      etimer_reset(&et_periodic);
    }
  }

  PROCESS_END();
}