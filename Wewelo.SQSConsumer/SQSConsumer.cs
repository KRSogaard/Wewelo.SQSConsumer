using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json.Linq;
using NLog;

namespace AmasTaskRunner
{
    public class SQSConsumerConfig
    {
        public String QueueUrl;
        public int Threads;
        public int FetchSize;
        public TimeSpan VisabilityTimeOut;

        public TimeSpan SleepWhenEmptyFetch = TimeSpan.FromSeconds(15);
        public TimeSpan SleepOnFailure = TimeSpan.FromSeconds(30);

        public override string ToString()
        {
            return "{" + (
                       $"\"queueUrl\": \"{QueueUrl}\", " +
                       $"\"threads\": \"{Threads}\", " +
                       $"\"fetchSize\": \"{FetchSize}\", " +
                       $"\"visabilityTimeOut\": \"{VisabilityTimeOut.TotalSeconds}\", " + 
                       $"\"sleepWhenEmptyFetch\": \"{SleepWhenEmptyFetch.TotalSeconds}\", " + 
                       $"\"sleepOnFailure\": \"{SleepOnFailure.TotalSeconds}\", ") + 
                   "}";
        }
    }
    
    public class SQSConsumer
    {
        private static Logger log = LogManager.GetCurrentClassLogger();
        
        private CancellationTokenSource cancellationTokenSource;
        private List<Task> consumers;
        private IAmazonSQS sqsClient;
        private SQSConsumerConfig config;

        private Dictionary<string, DateTime> processedMap;

        public SQSConsumer(IAmazonSQS sqsClient, SQSConsumerConfig config)
        {
            log.Info("SQS Consumer: " + config);
            
            this.consumers = new List<Task>();
            this.sqsClient = sqsClient;
            this.config = config;
        }

        public async Task Start(Func<string, Task> action)
        {
            log.Info($"Starting sqs consumer with {config.Threads} threads.");
            if (cancellationTokenSource != null)
            {
                log.Info("Consumer is already running, stopping threads and waiting for them to finish.");
                cancellationTokenSource.Cancel();
                await Task.WhenAll(consumers);
                log.Info("All SQS consumers are stopped, start will now continuing.");
                consumers.Clear();
            }
            
            processedMap = new Dictionary<string, DateTime>();
            
            cancellationTokenSource = new CancellationTokenSource();

            if (config.Threads == 1)
            {
                // This makes debugging easier
                LoopAcationTask(0, action);
            }
            else
            {
                Parallel.For(0, config.Threads, index =>
                {
                    LoopAcationTask(index, action);
                });
            }
            log.Info("All SQS Consumer have stopped.");
        }

        private void LoopAcationTask(int index, Func<string, Task> action)
        {
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                log.Info($"SQS Consumer threads {index} started.");
                try
                {
                    GetActionTask(action).Wait();
                }
                catch (Exception exp)
                {
                    log.Error(exp, $"SQS Consumer threads {index} had an exception.");
                }
            }
            log.Info($"SQS Consumer threads {index} stopped.");
        }

        public Task Stop()
        {
            cancellationTokenSource.Cancel();
            return Task.WhenAll(consumers);
        }

        private async Task GetActionTask(Func<string, Task> action)
        {
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                try
                {   
                    var request = new ReceiveMessageRequest(config.QueueUrl);
                    request.MaxNumberOfMessages = config.FetchSize;
                    request.VisibilityTimeout = (int)config.VisabilityTimeOut.TotalSeconds;

                    log.Trace($"Fetching {config.FetchSize} messages from \"{config.QueueUrl}\", visibility timeout is {config.VisabilityTimeOut.TotalSeconds} seconds.");
                    var messages = await sqsClient.ReceiveMessageAsync(request, cancellationTokenSource.Token);
                    
                    if (messages.HttpStatusCode != HttpStatusCode.OK)
                    {
                        log.Error(
                            $"SQS Queuery requset failed, will sleep {config.SleepOnFailure.TotalSeconds} seks before continuing on this thread. " +
                            "Status: " + messages.HttpStatusCode + " queue url: " + config.QueueUrl);
                        await Task.Delay(config.SleepOnFailure);
                        continue;
                    }

                    if (messages.Messages.Count > 0)
                    {
                        log.Info($"SQS consumer got {messages.Messages.Count} messages.");
                    }
                    
                    foreach (var message in messages.Messages)
                    {
                        if (message == null)
                        {
                            continue;
                        }

                        lock (processedMap)
                        {
                            if (processedMap.ContainsKey(message.MessageId))
                            {
                                log.Warn($"Got a duplication message, message {message.MessageId}.");
                                continue;
                            }
                            processedMap.Add(message.MessageId, DateTime.Now);

                            var keys = processedMap.Keys;
                            foreach (var key in keys)
                            {
                                if (processedMap[key] < DateTime.Now.AddMinutes(-5))
                                {
                                    processedMap.Remove(key);
                                }
                            }
                        }

                        try
                        {
                            await action(message.Body);
                        }
                        catch (Exception exp)
                        {
                            log.Error(exp, "Failed to process sqs message \"" + message.ReceiptHandle + "\": " + message.Body);
                        }

                        var deleteResponse = await sqsClient.DeleteMessageAsync(new DeleteMessageRequest(
                            config.QueueUrl, message.ReceiptHandle));
                        
                        if (deleteResponse.HttpStatusCode != HttpStatusCode.OK)
                        {
                            log.Error(
                                "Failed to delete message \"" + message.ReceiptHandle + "\" from the queue \"" + 
                                config.QueueUrl + "\".");
                        }
                        
                        if (cancellationTokenSource.IsCancellationRequested)
                        {
                            log.Warn("Cancellation Token is up, stopping further execution of messages.");
                            break;
                        }
                    }

                    if (messages.Messages.Count == 0)
                    {
                        log.Trace($"Last fetch did not have messages, sleeping {config.SleepWhenEmptyFetch.TotalSeconds} seks before trying again.");
                        await Task.Delay(config.SleepWhenEmptyFetch);
                    }
                }
                catch (Exception exp)
                {
                    log.Error(exp, "SQS Queuery requset failed with an exception, will sleep 15 sek before continuing on this thread." +
                        "Queue url: " + config.QueueUrl);
                    await Task.Delay(TimeSpan.FromSeconds(15));
                }
            }
            log.Warn("SQS Consumer thread stopped.");
        }
    }
}