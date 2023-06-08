# Backend-Instant-Messaging-System-Assignment

![Tests](https://github.com/sytan98/Backend-Instant-Messaging-System-Assignment/actions/workflows/test.yml/badge.svg)

This simple backend IM API is built on the demo and template provided from the 2023 TikTok Tech Immersion. It uses Go, the Kitex framework and Redis to provide an API that allows users to send or pull chats stored. Redis' sorted sets is used under the hood.

![plot](/architecture.png)

The default interface of the RPC server was used and sendtime used is the unix timestamp when message is received on the HTTP server.

Unit testing for the rpc-server was done together with miniredis to mock the Redis DB.

Load testing was done using Apache JMeter to verify if the API supports concurrent users > 20.

