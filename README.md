# wordCountKafkaStreaming
Given the finite sequence of text fragments (batches) arriving every 10 seconds, I should do the following: 

Use a stateful approach i.e. store the current values and update them at every batch processing. 

filter out the punctuations and the words with length less than 4 in all the batches, 

convert all the words to the lower case, 

and sort the WordCount result by count in descend order.  
