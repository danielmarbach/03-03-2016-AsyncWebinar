using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace AsyncDolls
{
    [TestFixture]
    public class AsyncScript
    {
        [Test]
        public async Task ThePump()
        {
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            var token = tokenSource.Token;

            var pumpTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    #region Output

                    "Pumping...".Output();

                    #endregion

                    await HandleMessage().ConfigureAwait(false);
                }
            });

            await pumpTask;

            tokenSource.Dispose();
        }

        static Task HandleMessage()
        {
            return Task.Delay(1000);
        }

        [Test]
        public async Task CaveatsOfTaskFactoryStartNew()
        {
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            var token = tokenSource.Token;

            Task<Task> pumpTask = Task.Factory.StartNew(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    #region Output

                    "Pumping...".Output();

                    #endregion

                    await HandleMessage().ConfigureAwait(false);
                }
            }, TaskCreationOptions.LongRunning);

            await pumpTask.Unwrap();

            tokenSource.Dispose();
        }

        // http://referencesource.microsoft.com/#mscorlib/system/threading/Tasks/ThreadPoolTaskScheduler.cs,57

        [Test]
        public async Task ConcurrentlyHandleMessages()
        {
            #region Cancellation AsAbove
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            var token = tokenSource.Token;
            #endregion

            var runningTasks = new ConcurrentDictionary<Task, Task>();

            var pumpTask = Task.Run(() =>
            {
                while (!token.IsCancellationRequested)
                {
                    #region Output

                    "Pumping...".Output();

                    #endregion

                    var runningTask = HandleMessage();

                    runningTasks.TryAdd(runningTask, runningTask);

                    runningTask.ContinueWith(t =>
                    {
                        #region Output

                        "... done".Output();

                        #endregion

                        Task taskToBeRemoved;
                        runningTasks.TryRemove(t, out taskToBeRemoved);
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }
            });

            await pumpTask;
            #region Output

            "Pump finished".Output();

            #endregion
            await Task.WhenAll(runningTasks.Values);
            #region Output

            "All receives finished".Output();

            #endregion
            tokenSource.Dispose();
        }

        [Test]
        public async Task LimitingConcurrency()
        {
            #region Cancellation AsAbove
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            var token = tokenSource.Token;
            #endregion
            #region Task Tracking AsAbove
            var runningTasks = new ConcurrentDictionary<Task, Task>();
            #endregion

            var semaphore = new SemaphoreSlim(2);

            var pumpTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    #region Output

                    "Pumping...".Output();

                    #endregion

                    await semaphore.WaitAsync().ConfigureAwait(false);

                    #region HandleMessage AsAbove

                    var runningTask = HandleMessage();

                    runningTasks.TryAdd(runningTask, runningTask);

                    #endregion

                    runningTask.ContinueWith(t =>
                    {
                        #region Output

                        "... done".Output();

                        #endregion

                        semaphore.Release();

                        #region Housekeeping AsAbove

                        Task taskToBeRemoved;
                        runningTasks.TryRemove(t, out taskToBeRemoved);

                        #endregion

                    }, TaskContinuationOptions.ExecuteSynchronously)
                    .Ignore();
                }
            });

            #region Awaiting completion AsAbove

            await pumpTask;

            #region Output

            "Pump finished".Output();

            #endregion

            await Task.WhenAll(runningTasks.Values);

            #region Output

            "All receives finished".Output();

            #endregion

            tokenSource.Dispose();

            #endregion
        }

        [Test]
        public async Task CancellingAndGracefulShutdown()
        {
            #region Cancellation AsAbove
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            var token = tokenSource.Token;
            #endregion
            #region Task Tracking AsAbove
            var runningTasks = new ConcurrentDictionary<Task, Task>();
            #endregion
            #region Limiting AsAbove

            var semaphore = new SemaphoreSlim(2);

            #endregion

            var pumpTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    #region Output

                    "Pumping...".Output();

                    #endregion

                    await semaphore.WaitAsync(token).ConfigureAwait(false);

                    #region HandleMessage AsAbove

                    var runningTask = HandleMessage();

                    runningTasks.TryAdd(runningTask, runningTask);

                    #endregion

                    #region Releasing Semaphore & Housekeeping AsAbove

                    runningTask.ContinueWith(t =>
                    {
                        #region Output

                        "... done".Output();

                        #endregion

                        semaphore.Release();

                        #region Housekeeping

                        Task taskToBeRemoved;
                        runningTasks.TryRemove(t, out taskToBeRemoved);

                        #endregion

                    }, TaskContinuationOptions.ExecuteSynchronously)
                        .Ignore();

                    #endregion
                }
            }, CancellationToken.None);

            await pumpTask.IgnoreCancellation();

            #region Awaiting completion

            #region Output

            "Pump finished".Output();

            #endregion

            await Task.WhenAll(runningTasks.Values);

            #region Output

            "All receives finished".Output();

            #endregion

            tokenSource.Dispose();

            #endregion
        }

        [Test]
        public async Task TheCompletePumpWithAsyncHandleMessage()
        {
            var runningTasks = new ConcurrentDictionary<Task, Task>();
            var semaphore = new SemaphoreSlim(100);
            var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var token = tokenSource.Token;
            int numberOfTasks = 0;

            var pumpTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await semaphore.WaitAsync(token).ConfigureAwait(false);
                    Interlocked.Increment(ref numberOfTasks);

                    var task = HandleMessage();

                    runningTasks.TryAdd(task, task);

                    task.ContinueWith(t =>
                    {
                        semaphore.Release();
                        Task taskToBeRemoved;
                        runningTasks.TryRemove(t, out taskToBeRemoved);
                    }, TaskContinuationOptions.ExecuteSynchronously)
                    .Ignore();
                }
            });

            await pumpTask.IgnoreCancellation();
            await Task.WhenAll(runningTasks.Values);
            tokenSource.Dispose();

            $"Consumed {numberOfTasks} messages with concurrency {semaphore.CurrentCount} in 10 seconds. Throughput {numberOfTasks/10} msgs/s".Output();
        }

        [Test]
        public async Task TheCompletePumpWithBlockingHandleMessage()
        {
            var runningTasks = new ConcurrentDictionary<Task, Task>();
            var semaphore = new SemaphoreSlim(100);
            var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var token = tokenSource.Token;
            int numberOfTasks = 0;

            var pumpTask = Task.Run(async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    await semaphore.WaitAsync(token).ConfigureAwait(false);

                    var runningTask = Task.Run(() =>
                    {
                        Interlocked.Increment(ref numberOfTasks);

                        return BlockingHandleMessage();
                    }, CancellationToken.None);

                    runningTasks.TryAdd(runningTask, runningTask);

                    runningTask.ContinueWith(t =>
                    {
                        semaphore.Release();

                        Task taskToBeRemoved;
                        runningTasks.TryRemove(t, out taskToBeRemoved);
                    }, TaskContinuationOptions.ExecuteSynchronously)
                    .Ignore();
                }
            });

            await pumpTask.IgnoreCancellation();
            await Task.WhenAll(runningTasks.Values);
            tokenSource.Dispose();

            $"Consumed {numberOfTasks} messages with concurrency {semaphore.CurrentCount} in 10 seconds. Throughput {numberOfTasks / 10} msgs/s".Output();
        }

        private static Task BlockingHandleMessage()
        {
            Thread.Sleep(1000);
            return Task.CompletedTask;
        }
    }

    static class TaskExtensions
    {
        public static void Ignore(this Task task)
        {
        }

        public static async Task IgnoreCancellation(this Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    static class StringExtensions
    {
        public static void Output(this string value)
        {
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss:fff") + ": " + value);
        }
    }
}
 
 