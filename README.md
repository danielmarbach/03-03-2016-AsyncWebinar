# Async/Await & Task Parallel Library webinar

Please read the [LICENSE.md](License) agreement

The font used in the slides is

[Kaffeesatz](https://www.yanone.de/fonts/kaffeesatz/)

# Outline

## ThePump

* A message pump is basically a while loop calling the method that needs to be executed
* The pump itself should be scheduled on a dedicated task since we don't want to block the calling thread with the pump operations. That's why we wrap the loop in a `Task.Run` call.
* The method we are calling inside the loop is called `HandleMessage`. It is an asynchronous operation since it doesn't block the calling thread. We simulate that work will take 1 second
* The pump loop needs to be aborted somehow when we want to shutdown our system (here finishing the unit test). One way to do this would be to use a flag. But in the async TPL world we have a construct called `CancellationTokenSource`. The source provides a `CancellationToken` which is linked to the source. As soon as the source is cancelled the token will transition into the cancelled state. The token can be queried for its state with `IsCancellationRequest` or you can use `ThrowIfCancellationRequested`.
* You could also pass the token to the Task.Run method. I'll be covering this later.
* Awaiting the pump task means we will await its completion.

## CaveatsOfTaskFactoryStartNew

* The message pump itself can be running for multiple minutes, hours even days before we shutdown the system. So the pump itself is a long running operation. The Task Parallel Library offers ways to hint the TPL about the operations that are running inside the task body. These hints can be passed into the more advanced API under `Task.Factory.StartNew`. For example there is a creation flag called `LongRunning`.
* The impact on the `LongRunning` flag is that the current TaskScheduler will [create a new background thread](http://referencesource.microsoft.com/#mscorlib/system/threading/Tasks/ThreadPoolTaskScheduler.cs,57) and assign that background task as the worker thread for the scheduled task. In the last webinar we talked about that every `await` statement is an opportunity for the worker thread to step out and do other things. When the body reaches the first await statement the newly created Thread will run to completion including all the associated resources like Stack, Memory etc.. So that means long running is a complete was for Tasks representing IO-bound work. Similar applies for the `AttachedToParent` creation options which I wont't cover in detail here.
* Another caveat is that if you declare a method `async` as the body of `StartNew` then this method returns a proxy Task (as `Task<Task>`). When you await the proxy, it will be completed eventhough the inner body is not yet completed. In order to await the inner body you have to `Unwrap` the proxy.
* This example shows that we should prefer `Task.Run` over `Factory.StartNew` unless we really know what we are doing.

## ConcurrentlyHandleMessages

In the intro we said that a message pump should handle messages concurrently. We can achieve concurrency, like explained in the first webinar, by not awaiting the `HandleMessage` method but instead acquire the task returned and track it. We are tracking the task for shutdown purposes. I made an explicit design choice to not just blindly shutdown but at least wait until all outstanding message handling methods are completed. In order to do that we need to track the tasks scheduled somehow. One option is to use a `ConcurrentDictionary`. We add the task to the concurrent dictionary. In order to cleanup the concurrent dictionary when a task is done we can schedule a continuation on the task which removes the task itself out of the dictionary. Since we now the removal is a short and synchronous operation we can hint the TPL with the `ExecuteSynchronously` flag. This is a kind request to the TPL to try to execute the continuation on the worker thread which handled the running task while the parent precedent task is completing. It is just a hint, no guarantee.  

## LimitingConcurrency

In the previous example we handled concurrency in an unbounded way. So essentially we scheduled task as fast as the machine allows us todo. What are the implications of this? Imagine you'd be opening up SqlConnections inside the handle method. We would be opening an unbound number of SqlConnections and quickly run out of connection in the underlying connection pool. There is also another low level aspect we need to deal with. Scheduled an unbounded number of tasks will lead to ramp up on the thread pool. A rampup means that the scheduler heuristics indicate that it needs more thread pool threads. Allocating more threads and its resources is a time consuming operation and might hurt more than it helps to improve throughput. To fine tune the pump we need a way to limit the number of concurrent operations.

If you google for tasks and concurrency limit you might stumble over []`LimitedConcurrencyLevelTaskScheduler`](https://msdn.microsoft.com/library/system.threading.tasks.taskscheduler.aspx). The problem of this special task scheduler is that although it is called limited concurrency it limits the number of threads associated to work on tasks. As we learned in the last webinar when we have tasks representing asynchronous IO-bound operations a single thread can easily handle hundreds of those tasks concurrently. What we need here is a Semaphore (Slide).

We use a `SemaphoreSlim` with two slots. The SemaphoreSlim allows us to asynchronously await free slots. Since we await the free slots the message pump will not continue to spin until there are slots available. Of course we need to release the semaphore to indicate free slots.

Since we now have an operation which is awaited inside the body of the task we need to mark the body as async. By marking it async the compiler will tell us that we are not awaiting the continuation task which we previously defined.

## CancellingAndGracefulShutdown

The previous code would actually asynchronously capture operations up to the number of slots available on the semaphore on the `WaitAsync` method. Therefore if we shutdown our message pump the pumpTask would "hang" potentially indefinitely. Well designed async API usually provide overloads which allow to pass in a CancellationToken. They will observe the cancellation token and throw an OperationCancelledException when the token is cancelled.

We could also pass the token into the Task.Run method. But passing the token into the Task.Run does't not mean the operation inside the task is automatically cancelled. It just means that no matter whether the body of the task throws or not the task will transition into the cancelled state when the token is cancelled.

Depending on the design we want to achieve we could also support cancellation inside the handle message method by floating the token into the handle method `HandleMessageWithCancellation`. By the way CancellationToken and TokenSources allow to build complex hierarchies of linked sources and tokens if we wan't to get fancy but I will not cover this here.

# Links
## About me
* [Geeking out with Daniel Marbach]( http://developeronfire.com/episode-077-daniel-marbach-geeking-out)

## Async / Await & TPL
* [Task Schedulers and Semaphores](https://blogs.msdn.microsoft.com/andrewarnottms/2016/02/06/taskschedulers-and-semaphores/)
* [LongRunning is useless for Task.Run with async/await](https://blog.i3arnon.com/2015/07/02/task-run-long-running/)
* [StartNew is Dangerous](http://blog.stephencleary.com/2013/08/startnew-is-dangerous.html)
* [TaskCreationOptions.AttachedToParent is not waiting for child task](http://stackoverflow.com/questions/14150448/taskcreationoptions-attachedtoparent-is-not-waiting-for-child-task)
* [New TaskCreationOptions and TaskContinuationOptions in .NET 4.5](http://blogs.msdn.com/b/pfxteam/archive/2012/09/22/new-taskcreationoptions-and-taskcontinuationoptions-in-net-4-5.aspx)
