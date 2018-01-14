A Java library to convert a sequence of ActiveMQ JMS messages into a reactive stream. Exposes methods to retrieve an RxJava Observable or Flowable from the message sequence.

Once we retrieve an Observable / Flowable, the ActiveMQ JMS messages can be manipulated using standard reactive stream methods, eg. map, filter, flatMap, zip, window, scan, and so on.

For usage, please browse through the sample programs in the sample directory.

There is also a unit test for validating the functionality with an Observable and Flowable.
