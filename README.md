# PseudoLambda
## A mock of AWS Lambda for serverless code execution

This project has been an undertaking to familiarize myself and learn the basics of the major problems and ideas behind distributed computing. This includes leader election, task scheduling, logging, fault tolerance, and more!

The system uses ZooKeeper for leader election, a Round-Robin scheduling system, and requests are handled through an HTTP gateway - requests can be submitted with tools like HTTPie (which I used personally in testing).

It works out of the box, pull the code and the tests run immediately. Examples of starting a cluster and submitting work are included, as well as various tests to show various system states, including but not limited to cache hits and misses, submitted code not executing properly, and leader re-election in the case of a fault.

Logging is done in a custom formatting to allow for easy readability and understanding.

I hope you enjoy this project! :)