#include "contiki.h"
#include "net/routing/routing.h"
#include "random.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define WITH_SERVER_REPLY  1
#define UDP_CLIENT_PORT	8765
#define UDP_SERVER_PORT	5678

#define LIMIT_PING_PONG 5

static struct simple_udp_connection udp_conn;

#define START_INTERVAL		(15 * CLOCK_SECOND)
#define SEND_INTERVAL		  (30 * CLOCK_SECOND)

static struct simple_udp_connection udp_conn;

typedef struct payload{
  uint8_t command;
  uint8_t data;
}payload_t;

typedef struct response{
  //error_code: 
  //-1 -> ERROR
  // 0 -> WRITE OK
  // 1 -> READ RESPONSE
  int8_t code;
  uint8_t data;
}response_t;

/*---------------------------------------------------------------------------*/
PROCESS(udp_client_process, "UDP client");
AUTOSTART_PROCESSES(&udp_client_process);
/*---------------------------------------------------------------------------*/
static void
udp_rx_callback(struct simple_udp_connection *c, const uip_ipaddr_t *sender_addr, uint16_t sender_port,
  const uip_ipaddr_t *receiver_addr, uint16_t receiver_port, const uint8_t *data, uint16_t datalen)
{
  response_t response = *(response_t *)data;
  if(response.code == -1){
    LOG_INFO("Received error code %d from ", response.code);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
  }else if(response.code == 0){
    LOG_INFO("Write done!");
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
  }
  else{
    LOG_INFO("Data read: %u!", response.data);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
  }
  
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_client_process, ev, data)
{
  static struct etimer periodic_timer;
  static struct etimer lock_timer;
  static payload_t request;
  static uip_ipaddr_t dest_ipaddr;

  PROCESS_BEGIN();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  etimer_set(&periodic_timer, random_rand() % SEND_INTERVAL);
  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    if(NETSTACK_ROUTING.node_is_reachable() && NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr)) {
      /* Send to DAG root */

      //LOCK
      request.command = 0;
      LOG_INFO("Sending request %u to ", request.command);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &request, sizeof(request), &dest_ipaddr);

      etimer_set(&lock_timer, 3*CLOCK_SECOND);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&lock_timer));
      
      //WRITE
      request.command = 1;
      request.data = random_rand() % 20; //random value between 0 and 19
      LOG_INFO("Sending request %u to ", request.command);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &request, sizeof(request), &dest_ipaddr);

      etimer_set(&lock_timer, CLOCK_SECOND);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&lock_timer));

      //READ
      request.command = 2;
      LOG_INFO("Sending request %u to ", request.command);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &request, sizeof(request), &dest_ipaddr);

      //LOCK
      request.command = 0;
      LOG_INFO("Sending request %u to ", request.command);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &request, sizeof(request), &dest_ipaddr);

      etimer_set(&lock_timer, 10*CLOCK_SECOND);
      PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&lock_timer));
    } else {
      LOG_INFO("Not reachable yet\n");
    }

    /* Add some jitter */
    etimer_set(&periodic_timer, SEND_INTERVAL
      - CLOCK_SECOND + (random_rand() % (10 * CLOCK_SECOND)));
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
