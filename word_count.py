from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os
import re
BATCH_TIMEOUT = 5


# filter words with length less than 4 in all the batches
def filter_word(input_word):
    if (len(input_word) <= 4):
        return False
    else:
        return True


# aggregrate function to sum words and counts for them
def aggregrateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)



if __name__ == "__main__":


    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, BATCH_TIMEOUT)

    # Preparing batches with the input data
    DATA_PATH = "input_directory"
    batches = [sc.textFile(os.path.join(DATA_PATH, path)) for path in os.listdir(DATA_PATH) if os.path.isfile(os.path.join(DATA_PATH, path)) ]

    # Creating Dstream to emulate realtime data generating
    dstream = ssc.queueStream(rdds=batches)


    # Preparing batches with the input_directory data
    ssc.checkpoint("checkpoint")


    running_counts = dstream.flatMap(lambda line: re.split("\W*\s+\W*", line.strip(), flags=re.UNICODE))\
                  .filter(filter_word)  \
                  .map(lambda x: (x.lower(), 1))\
                  .updateStateByKey(aggregrateFunc)

    # sort the dstream for current batch
    sorted_counts = running_counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    sorted_counts.pprint()

    ssc.start()
    ssc.awaitTermination()