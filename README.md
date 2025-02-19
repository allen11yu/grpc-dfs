# gRPC and Distributed File System

**This project was developed as part of the CS6200: Introduction to Operating Systems course at Georgia Tech. It demonstrates my implementation of synchronous and asynchronous gRPC for file management. Please note that this project is for educational purposes, and copying or using the code without permission is prohibited. If you have any concerns, feel free to reach out to me.**

## Part 1: Synchronous gRPC
In part 1, the objective is to implement an array of remote procedure calls (RPC). Specifically, the RPCs are fetch, store, delete, list and stat. The fetch method enables the client to fetch files from a remote server. The store method enables the client to store files to the server. The list method enables the client to request a list of files along with their attributes currently existing on the server. Lastly, the stat method enables the client to request a particular file’s attributes from the server.

Given that the protocol service is synchronous, I feel like the implementation would be fairly straightforward. That being said, I mostly referenced [gRPC’s basics tutorial](https://grpc.io/docs/languages/cpp/basics/) in aid of developing the RPC methods. Furthermore, I learned about protocol buffers, message types and various scalar types from the [proto 3 language guide](https://protobuf.dev/programming-guides/proto3/). I also learned about deadline implementations from the [deadline guide](https://grpc.io/blog/deadlines/).

In both the fetch and store methods, the client is sending data to the server and the server is sending data to the client. Sending data over RPC requires chunking behavior. In other words, the more practical approach than just sending all the data at once is to break data into chunks and send those chunks continuously. To implement the chunking behavior, we need to define the “sending” side of fetch and store RPC as streaming. For example, in project 4, the sender of the fetch method is the server and the sender of the store method is the client. Here, we should set server as streaming for fetch method and client as streaming for store method.

One challenge I have when working on this part is having little experience in C++. At first glance of the code base, I was confused by the syntax thus having trouble connecting components together. To overcome this, I watched a 30 minute [C++ tutorial](https://www.youtube.com/watch?v=0NwsayeOsd4) on YouTube to grasp C++ fundamentals.

## Part 2: Asynchronous gRPC
In part 2, the main objective is to enable asynchronous calls on top of synchronous gRPC, so that the clients’ and servers’ caches are synchronized. To do so, we need the power of mutex to prevent race conditions and write locks to enable one writer per file. Here, I represent the write locks as a map where file name (key) maps to client ID (value). Since there will be multiple clients, we can only allow one client to access one file. This means that the lock is granted when file name maps to nothing. However, after careful consideration, there will also be a case where the same client will request the same lock after acquiring. I adjust the if condition to address this edge case.

One optimization I made is to represent write locks as an unordered map as opposed to regular map (ordered). In my if condition, I am only checking the existence of the client ID, so the sorting feature is unnecessary. Since the unordered map offers a hash table structure and lower memory overhead due to not sorting, using that seems like a good choice.

Another optimization I made is to update modified time to be equal when the CRC value is equal. Since the comparison relies on the modified time, if we don’t update the modified time, the client will repetitively check the files that are the same due to difference in modified time. Overtime, this will negatively impact performance by increasing memory overheads by storing temporary buffers. In the real world, the redundant check will also increase network overhead since the files are stored remotely on the cloud.

One challenge I had is experiencing SEGVAL error causing the server to crash after a few loops file listing. Since SEGVAL error is associated with reading NULL addresses, I added some if conditions to check if I am accessing a NULL pointer. Turns out, I am reading NULL directories. This means that the directory is not open properly, so I print out the errno value which is 24. Then, I researched about the meaning of errno values and I learned that the opened file descriptor limit is reached. This issue was caused by not closing the directories after each read. As a result, the old directories pile up and cross the limit mark.



