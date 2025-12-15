#include "contiki.h"
#include "net/routing/routing.h"
#include "random.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"

#include "sys/log.h"
#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define WITH_SERVER_REPLY 1
#define UDP_CLIENT_PORT 8765
#define UDP_SERVER_PORT 5678

#define START_INTERVAL (15 * CLOCK_SECOND)
#define SEND_INTERVAL (60 * CLOCK_SECOND)
#define TIMER_TIME (60 * CLOCK_SECOND)

#define MAX_ITERATIONS 10

static struct simple_udp_connection udp_conn;
static struct etimer timer;

static int done = 0;
static int last_value = 0;

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
  etimer_restart(&timer);
  LOG_INFO("Callback reached!\n");

  unsigned value;
  // CORREZIONE: Usa memcpy per evitare crash da memoria non allineata
  if(datalen >= sizeof(unsigned)) {
      memcpy(&value, data, sizeof(unsigned));
  } else {
      LOG_INFO("Received packet too short\n");
      return;
  }

  // Controllo fine iterazioni
  if (value > MAX_ITERATIONS) { // Nota: controlla 'value', non 'last_value' qui
    LOG_INFO("Finished with count %u!\n", value);
    etimer_stop(&timer);
    done = 1;
    return;
  }
  
  LOG_INFO("Received response %u from ", value);
  LOG_INFO_6ADDR(sender_addr);
  LOG_INFO_("\n");

  if ((last_value + 1) == value) {
    last_value = value + 1;
    
    LOG_INFO("Sending response %u to ", last_value);
    LOG_INFO_6ADDR(sender_addr);
    LOG_INFO_("\n");
    simple_udp_sendto(&udp_conn, &last_value, sizeof(last_value), sender_addr);

  } else {
    LOG_INFO("Ignore wrong packet in sequence order! Expected %d but got %d\n", last_value + 1, value);
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(udp_client_process, ev, data)
{
  static struct etimer periodic_timer;
  static unsigned count = 0;
  uip_ipaddr_t dest_ipaddr;

  PROCESS_BEGIN();

  /* Initialize UDP connection */
  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  /*
    first of all we need to check if the server is reachable,
    by sending a udp_request to it. If it respond, it is reachable.

    this is the reachment of the root of the tree
  */
  etimer_set(&periodic_timer, random_rand() % SEND_INTERVAL);
  while (1)
  {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    if (NETSTACK_ROUTING.node_is_reachable() && NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr))
    {
      /* Send to DAG root */
      LOG_INFO("Sending request %u to ", count);
      LOG_INFO_6ADDR(&dest_ipaddr);
      LOG_INFO_("\n");
      etimer_set(&timer, TIMER_TIME);
      simple_udp_sendto(&udp_conn, &count, sizeof(count), &dest_ipaddr);
      // etimer_stop(&periodic_timer);
      break;
    }
    else
    {
      LOG_INFO("Not reachable yet\n");
    }

    /* Add some jitter */
    etimer_set(&periodic_timer, SEND_INTERVAL - CLOCK_SECOND + (random_rand() % (2 * CLOCK_SECOND)));
  }

  while (1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    if (done == 1) {
      break;
    } else {
      etimer_set(&timer, CLOCK_SECOND * 60);
      LOG_INFO("Timer expired, packet lost. Restart ping pong.\n");
      count = 0;
      simple_udp_sendto(&udp_conn, &count, sizeof(count), &dest_ipaddr);
    }
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
