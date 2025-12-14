# Evaluation lab - Akka

## Group number: 09

## Group members

- Galimberti Matteo
- Komisarjevsky Luca
- Ghisolfi Davide

## Description of message flows
Here are the possible messages sent:
- ConfigMsg &rarr; this is the message that is used to configure the AddressBookClientActor passing the balancer to it. The ClientActor will save the reference to the balancer
- ConfigBalancerMsg &rarr; this is the message that is used to configure AddressBookBalancerActor passing the 2 workers as reference and saving them in the balancer.
- GenericReplyMsg &rarr; this is a generic message that is used to store a reply from the balancer to the client including both messages of type ReplyMsg and TimeoutMsg.
- ReplyMsg &rarr; this message is used by both balancer and workers to send back a "matching" result (email).
- TimeoutMsg &rarr; this message is used by the balancer to tell the client that the execution took too long so both workers are sleepy.
- PutMsg.
- GetMsg &rarr; we added a new attribute "isPrimary" used by the AddressBookBalancerActor to tell the worker if the entry is primary or not (just to save some execution time).
- RestMsg.
- ResumeMsg.

We assumed that the put message is not a query, so if a worker is in the "RestMode" it still saves new addresses. It doesn't reply to queries.

Inside AddressBookBalancerActor the method "routeQuery" uses the attribute "isPrimary" just to tell the worker if it needs to look in the primary or replicated storage. We used the AskPattern and we leveraged the use of TimeoutException to tell if the worker is sleepy and eventually query the replicated one. If both workers timeout, balancer sends a TimeoutMsg that the client will handle.