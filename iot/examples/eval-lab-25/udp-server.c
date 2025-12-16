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
#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT	8765
#define UDP_SERVER_PORT	5678

#define CTIMER_INTERVAL (5 * CLOCK_SECOND)

typedef struct payload{
  uint8_t command;
  uint8_t data;
}payload_t;

typedef struct response{
  //code: 
  //-1 -> ERROR
  // 0 -> WRITE OK
  // 1 -> READ RESPONSE
  int8_t code;
  uint8_t data;
}response_t;

static uip_ipaddr_t lock_owner;
static uip_ipaddr_t zero_ip;

static uint8_t data_structure = 0;

static struct simple_udp_connection udp_conn;

static void ctimer_callback(void *data);
static struct ctimer timeout_ctimer;

PROCESS(udp_server_process, "UDP server");
AUTOSTART_PROCESSES(&udp_server_process);
/*---------------------------------------------------------------------------*/

static void ctimer_callback(void * data){
  //5 seconds timer expired
  LOG_INFO("Lock expired!\n");
  uip_ipaddr(&lock_owner, 0,0,0,0);
}

/*---------------------------------------------------------------------------*/
static void
udp_rx_callback(struct simple_udp_connection *c, const uip_ipaddr_t *sender_addr, uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr, uint16_t receiver_port, const uint8_t *data,uint16_t datalen)
{

  payload_t request = *(payload_t *)data;
  response_t response;
  LOG_INFO("Received request %u from ", request.command);
  LOG_INFO_6ADDR(sender_addr);
  LOG_INFO_("\n");

  if(request.command == 2){
    //READ
    LOG_INFO("Sending data %u to ", data_structure);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");

    response.code = 1;
    response.data = data_structure;

    simple_udp_sendto(&udp_conn, &response, sizeof(response), sender_addr);
  }
  else if(request.command == 0){
    //LOCK
    if(uip_ipaddr_cmp(&zero_ip, &lock_owner)){
      //LOCK is free
      //assigning lock to sender
      uip_ipaddr_copy(&lock_owner, sender_addr);
      ctimer_set(&timeout_ctimer, (clock_time_t) CTIMER_INTERVAL, ctimer_callback, "a");
    }
    else{
      LOG_INFO("Already locked\n");
    }
  }
  else if(request.command == 1){
    //WRITE
    if(uip_ipaddr_cmp(&lock_owner, sender_addr)){
      //this is the lock owner
      LOG_INFO("Write successful");
      data_structure = request.data;
      ctimer_stop(&timeout_ctimer);
      uip_ipaddr(&lock_owner, 0,0,0,0);
      response.code = 0;
      simple_udp_sendto(&udp_conn, &response, sizeof(response), sender_addr);
    }
    else{
      //ERROR
      response.code = -1;
      LOG_INFO("Impossible to write\n");
      simple_udp_sendto(&udp_conn, &response, sizeof(response), sender_addr);
    }
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_server_process, ev, data)
{
  PROCESS_BEGIN();

  //Initialize lock
  //set to 0 the IPv6
  uip_ipaddr(&lock_owner, 0,0,0,0);
  uip_ipaddr(&zero_ip, 0,0,0,0);

  /* Initialize DAG root */
  NETSTACK_ROUTING.root_start();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_SERVER_PORT, NULL,
                      UDP_CLIENT_PORT, udp_rx_callback);

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
