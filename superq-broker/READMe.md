File storage format:
 For each queue there will be a file
increasing order of sequence:
 - Each message will be appended in the end
 - The message will be fetched based upon some initial number, 
 number of message to be fetched, filtering of message based upon 
 few columns
    - problem with this approach -  When there will be gap between 
    initial number and filtered column, this will take time
    



Calculation:
    If we have 1000 active queue and there are roughly 4 consumers
    per queue then here is what happens.
    - for each consumption, the acknowledgement will be received 
    and on each acknowledgement we need to delete the message
    so on an average 1000*4 delete request will be coming.
    - Once prefetched message got consumed in the memory next batch will be fetched. Next batch of message
    should not have following categories of the messages
     - for which ack is waiting
     - all the message for which ack received
     
    Default strategy is to filter the message from the last offset. will be very in efficient if there is long
    gaps.
    
    We can shift the messages. For each fetch of next batch following will happen
     - Its buggy and there will be a lot of write
     
     Second strategy
      Create another message 
     
----
Functional Requirement:
   - registrationation of connection
      - Reply of a connection in terms of broker info
      - 
     
     
 