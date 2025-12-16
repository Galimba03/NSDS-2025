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

static struct simple_udp_connection udp_conn;

#define START_INTERVAL		(15 * CLOCK_SECOND)
#define TIMEOUT_TIMER		  (60 * CLOCK_SECOND)

#define MAX_MESSAGES      5

static struct simple_udp_connection udp_conn;

/*---------------------------------------------------------------------------*/
PROCESS(udp_client_process, "UDP client");
AUTOSTART_PROCESSES(&udp_client_process);
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
  unsigned count = *(unsigned *)data;

  if (count != MAX_MESSAGES) {
    LOG_INFO("Received response %u from ", ++count);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
    simple_udp_sendto(&udp_conn, &count, sizeof(count), sender_addr);
  } else {
    LOG_INFO("Received response %u from ", ++count);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_client_process, ev, data)
{
  static struct etimer periodic_timer;
  static unsigned count;
  uip_ipaddr_t dest_ipaddr;

  PROCESS_BEGIN();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);



  etimer_set(&periodic_timer, TIMEOUT_TIMER);
  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    if (count == MAX_MESSAGES) {
      count = 0;
      LOG_INFO("[All went well]. Sending request %u to ", count);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &count, sizeof(count), &dest_ipaddr);
    } else {
      count = 0;
      LOG_INFO("[All went wrong]. Sending request %u to ", count);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      simple_udp_sendto(&udp_conn, &count, sizeof(count), &dest_ipaddr);
    }

    etimer_set(&periodic_timer, TIMEOUT_TIMER);
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
