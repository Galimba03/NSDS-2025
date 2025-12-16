/*
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
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

#define UDP_CLIENT_PORT	8765
#define UDP_SERVER_PORT	5678

#define CLIENT_TIMEOUT_MINS 3

static uip_ipaddr_t receivers[MAX_RECEIVERS];
static uip_ipaddr_t parent_addrs[MAX_RECEIVERS];
//static uint8_t known_parents = 0;
static uint8_t clients_ttl[MAX_RECEIVERS];

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

  static uint8_t i = 0;
  static uint8_t actual_pos = 0;
  static uint8_t found = 0;
  static struct etimer client_timer;

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

      clients_ttl[known_receivers] = CLIENT_TIMEOUT_MINS;
      known_receivers++;
    }else{
      return;
    }
  }
  else{
    if(!uip_ipaddr_cmp(&parent_addrs[actual_pos], &parent)){
      uip_ipaddr_copy(&parent_addrs[actual_pos], &parent);
    }
    clients_ttl[actual_pos] = CLIENT_TIMEOUT_MINS;
  }

  LOG_INFO("Received parent ");
  LOG_INFO_6ADDR(&parent); // Stampa l'indirizzo parent
  LOG_INFO_(" from ");
  LOG_INFO_6ADDR(sender_addr); // Stampa l'indirizzo mittente
  LOG_INFO_("\n");
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  static struct etimer periodic_timer;

  PROCESS_BEGIN();

    static uint8_t i=0;
  /* Initialize temperature buffer */
  for (i=0; i<MAX_RECEIVERS; i++) {
    uip_ipaddr(&receivers[i], 0,0,0,0);
    uip_ipaddr(&parent_addrs[i], 0,0,0,0);
    clients_ttl[i] = 0;
  }  

  /* Initialize DAG root */
  NETSTACK_ROUTING.root_start();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  etimer_set(&periodic_timer, random_rand() % (CLOCK_SECOND * 60));
  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    
    LOG_INFO("--- Checking Active Clients (Total: %d) ---\n", known_receivers);

    // Iteriamo attraverso i client per gestire il timeout
    i = 0;
    while(i < known_receivers) {
        
        // Decrementiamo il TTL
        if(clients_ttl[i] > 0) {
            clients_ttl[i]--;
        }

        // Se il TTL è arrivato a 0, rimuoviamo il client
        if(clients_ttl[i] == 0) {
            LOG_INFO("Client ");
            LOG_INFO_6ADDR(&receivers[i]);
            LOG_INFO_(" timed out. Removing.\n");

            // RIMOZIONE:
            // Per rimuovere un elemento da un array senza lasciare buchi, 
            // spostiamo l'ultimo elemento dell'array nella posizione corrente (i).
            if (i != known_receivers - 1) {
                // Copia l'IP dell'ultimo sopra quello corrente
                uip_ipaddr_copy(&receivers[i], &receivers[known_receivers - 1]);
                // Copia il parent dell'ultimo sopra quello corrente
                uip_ipaddr_copy(&parent_addrs[i], &parent_addrs[known_receivers - 1]);
                // Copia il TTL dell'ultimo sopra quello corrente
                clients_ttl[i] = clients_ttl[known_receivers - 1];
            }
            
            // Decrementiamo il numero totale di client
            known_receivers--;
            
            // NON incrementiamo 'i' qui, perché dobbiamo controllare il 
            // nuovo elemento che abbiamo appena spostato in questa posizione
            // (quello che prima era l'ultimo).
        } else {
            // Se il client è ancora vivo, stampiamo info e passiamo al prossimo
            LOG_INFO("Client ");
            LOG_INFO_6ADDR(&receivers[i]); 
            LOG_INFO_(" is active (TTL: %d min)\n", clients_ttl[i]);
            i++;
        }
    }

    // Reimpostiamo il timer per il prossimo minuto (+ un po' di jitter casuale per evitare collisioni)
    etimer_set(&periodic_timer, (CLOCK_SECOND * 60) 
      - CLOCK_SECOND + (random_rand() % (2 * CLOCK_SECOND)));
  }

  
  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
