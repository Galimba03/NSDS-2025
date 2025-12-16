/*
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the Institute nor the names of its contributors
 * may be used to endorse or promote products derived from this software
 * without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * This file is part of the Contiki operating system.
 *
 */

#include "contiki.h"
#include "net/routing/routing.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
#include "random.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO
#define MAX_RECEIVERS 100

#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678

// *** MODIFICA: Definizione del timeout in Ticks del processore (3 minuti) ***
#define TIMEOUT_TICKS (3 * 60 * CLOCK_SECOND)

static uip_ipaddr_t receivers[MAX_RECEIVERS];
static uip_ipaddr_t parent_addrs[MAX_RECEIVERS];

static clock_time_t last_seen[MAX_RECEIVERS];

static uint8_t known_receivers = 0;

static struct simple_udp_connection udp_conn;

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);
/*---------------------------------------------------------------------------*/
static void
udp_rx_callback(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  uip_ipaddr_t parent = *(uip_ipaddr_t *)data;

  // Nota: ho rimosso 'static' da queste variabili perché è più sicuro 
  // reinizializzarle a ogni chiamata, specialmente 'found'.
  uint8_t i = 0;
  uint8_t actual_pos = 0;
  uint8_t found = 0; 

  for(i = 0; i < known_receivers; i++){
    if(uip_ipaddr_cmp(sender_addr, &receivers[i])){
      found = 1;
      actual_pos = i;
      break;
    }
  }

  if(!found){
    if(known_receivers < MAX_RECEIVERS){
      uip_ipaddr_copy(&receivers[known_receivers], sender_addr);
      uip_ipaddr_copy(&parent_addrs[known_receivers], &parent);

      last_seen[known_receivers] = clock_time();
      
      known_receivers++;
    }else{
      return;
    }
  }
  else{
    if(!uip_ipaddr_cmp(&parent_addrs[actual_pos], &parent)){
      uip_ipaddr_copy(&parent_addrs[actual_pos], &parent);
    }
    last_seen[actual_pos] = clock_time();
  }

  LOG_INFO("Received parent ");
  LOG_INFO_6ADDR(&parent); 
  LOG_INFO_(" from ");
  LOG_INFO_6ADDR(sender_addr); 
  LOG_INFO_("\n");
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  static struct etimer periodic_timer;
  static uint8_t i = 0;
  // Variabile per leggere l'ora corrente nel loop
  clock_time_t now; 

  PROCESS_BEGIN();

  /* Initialize buffers */
  for (i=0; i<MAX_RECEIVERS; i++) {
    uip_ipaddr(&receivers[i], 0,0,0,0);
    uip_ipaddr(&parent_addrs[i], 0,0,0,0);
    last_seen[i] = 0;
  }  

  /* Initialize DAG root */
  NETSTACK_ROUTING.root_start();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  etimer_set(&periodic_timer, random_rand() % (CLOCK_SECOND * 60));
  
  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    
    now = clock_time();

    LOG_INFO("--- Checking Active Clients (Total: %d, Time: %lu) ---\n", known_receivers, now);

    // Iteriamo attraverso i client per gestire il timeout
    i = 0;
    while(i < known_receivers) {
        
        // *** MODIFICA: Calcoliamo la differenza di tempo ***
        // Se (Adesso - Ultima volta) > 3 minuti -> Timeout
        if(now - last_seen[i] > TIMEOUT_TICKS) {
            
            LOG_INFO("Client ");
            LOG_INFO_6ADDR(&receivers[i]);
            LOG_INFO_(" timed out (Last seen %lu ticks ago). Removing.\n", now - last_seen[i]);

            // RIMOZIONE: Swap con l'ultimo elemento
            if (i != known_receivers - 1) {
                uip_ipaddr_copy(&receivers[i], &receivers[known_receivers - 1]);
                uip_ipaddr_copy(&parent_addrs[i], &parent_addrs[known_receivers - 1]);
                
                // *** Copiamo anche il timestamp dell'ultimo elemento ***
                last_seen[i] = last_seen[known_receivers - 1];
            }
            
            known_receivers--;
            
            // NON incrementiamo 'i' qui, ricontrolliamo la posizione corrente
            // che ora contiene un nuovo elemento (quello scambiato).
        } else {
            // Se il client è ancora vivo
            // (Opzionale) Stampiamo da quanto tempo è attivo per debug
            // LOG_INFO("Client OK (Seen %lu ticks ago)\n", now - last_seen[i]);
            i++;
        }
    }

    // Reimpostiamo il timer
    etimer_set(&periodic_timer, (CLOCK_SECOND * 60) 
      - CLOCK_SECOND + (random_rand() % (2 * CLOCK_SECOND)));
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/