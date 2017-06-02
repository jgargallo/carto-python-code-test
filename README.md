# carto-python-code-test

Tested on **python 3.5.2**

The first approach that came up to my mind was to apply map/reduce. By Taking advantage of **Range** header and **async**, we can download the file in parallel without being blocked by IO.

The expected approach would be doing streaming so you can process lines as they are downloaded, and it would be the most efficient one in terms of *memory*, but would it be the fastest one?

Instead of streaming, the whole chunk for every parallel download is serialized and sent to a different process by using [multiprocessing](https://docs.python.org/2/library/multiprocessing.html). With this approach we make the most of our multiple cores avoiding our GIL friend. My main concern here was about the time consumed by serialization.

Take into account code's design is based on performance, not robustness (there's room for improvement).

Really enjoyed it!
