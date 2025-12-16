# Evaluation lab - Contiki-NG

## Group number: 09

## Group members

- Luca Komisarjevsky - 10861704
- Matteo Galimberti - 10819040
- Davide Ghisolfi - 10839162

## Solution description
In the solution you can find 3 ".c" files, the server and 2 clients that do different operations to test different operations of the server. One client is testing if the lock mechanism and the write is correct. One client is testing if the reads operations are executed correclty.

In the simulation we included 1 server, and 3 clients: respectivly 2 for writing and locking and 1 for reading.

In the server and client we created data stuctures (payload_t and response_t) for the message passing. From line 90 to line 130 we hadle all the possible request operations, in particolar we used a ctimer for the manage of the callback in case a client occupies too much the lock (the callback obviously free the lock that was taken for too long).

Clients try to generate all possible cases of situations created (i.e. the client that lock and the other that tries to write without the lock acquired).