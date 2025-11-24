# Evaluation lab - Node-RED

## Group number: 09

## Group members

- Luca Komisarjevsky 10861704
- Davide Ghisolfi 10839162
- Matteo Galimberti 10819040

## Description of message flows

BOT link: https://t.me/eval_lab_25_09_bot
API Key: 8383568649:AAE7L-81pH6Yo3A3zhv17d-6HMAn6UDkFuI

The telegram message passes through the "Command Parser" which parses the message and outputs a parsed new message in the correct format for the following blocks.
We used a context variable to associate to the user chat id an array of arrays of 2 elements, respectively City and Country in order to keep each user list separated.

In case of QUERY or REPORT message, the flow goes to an "openweathermap" node that then goes into a text node that generates the final message sent to the user.

In case of REPORT message, the Report sender node generates an array of messages that are sent one after the other to the "openweathermap" node.

The program does not check if there are duplicates in the TRACKING list.

## Extensions 
None