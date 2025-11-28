using MoreLinq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Notifying
{
    public record SourceBlockEvent;

    public record DeliveringMessagesEvent(int Count): SourceBlockEvent;

    public record ReservationReleasedEvent() : SourceBlockEvent;

    public record SourceBlockNotificationHooks<T>(ISourceBlock<T> SourceBlock)
    {
        /// <summary>
        /// This Action will be executed when a messages are sent from the source block.
        /// Make sure the Action terminates immediately, to prevent blocking the thread delivering messages.
        /// </summary>
        public Action<DeliveringMessagesEvent>? OnDeliveringMessages { get; set; }

        /// <summary>
        /// This Action will be executed when a linked target released its reservation.
        /// Make sure the Action terminates immediately, to prevent blocking the thread delivering messages.
        /// </summary>
        public Action<ReservationReleasedEvent>? OnReservationReleased { get; set; }

        public Action? OnCompleting { get; set; }

        public Task Completion { get; set; } = Task.CompletedTask;

        /// <summary>
        /// Setting this to true will make the hooks run in the thread of the SourceBlock. Make sure not to block it.
        /// </summary>
        public bool NotifySynchronously { get; set; }
    }

    public static class SourceBlockNotificationHooksExtensions
    {
        public static SourceBlockNotificationHooks<T> Build<T>(this SourceBlockNotificationHooks<T> h)
        {
            var res = h with { };
            if (!h.NotifySynchronously)
            {
                var innerHooks = res with { NotifySynchronously = false };
                BufferBlock<SourceBlockEvent> eventDeliveryQueue = new();
                async Task DeliverMessagesAsynchronously()
                {
                    while (await eventDeliveryQueue.OutputAvailableAsync())
                    {
                        if (eventDeliveryQueue.TryReceiveAll(out var msgs))
                        {
                            innerHooks.DispatchEvents(msgs);
                        }
                    }
                }
                var deliveryTask = Task.Run(DeliverMessagesAsynchronously);

                res.OnDeliveringMessages = e => eventDeliveryQueue.Post(e);
                res.OnReservationReleased = e => eventDeliveryQueue.Post(e);
                res.OnCompleting = () =>
                {
                    eventDeliveryQueue.Complete();
                    innerHooks?.OnCompleting?.Invoke();
                };
                res.Completion = Task.WhenAll(deliveryTask, innerHooks.Completion);
            }
            return res;
        }


        public static void DispatchEvents<T>(this SourceBlockNotificationHooks<T> hooks, IEnumerable<SourceBlockEvent> events)
        {
            foreach (var evs in events.GroupAdjacent(e => e.GetType()))
            {
                if (evs.Key == typeof(DeliveringMessagesEvent))
                {
                    //For optimization purposes, we sum up all delivering messages events into one.
                    var totalCount = evs.Cast<DeliveringMessagesEvent>().Sum(e => e.Count);
                    hooks.OnDeliveringMessages!.Invoke(new(totalCount));
                }
                else if (evs.Key == typeof(ReservationReleasedEvent))
                {
                    //For optimization purposes, we only deliver one ReservationReleaseEvent in case of multiple consecutive ones.
                    //This is fine, because releasing a reservation consecutively is idempotent.
                    hooks.OnReservationReleased!.Invoke(new());
                } else
                {
                    throw new InvalidOperationException($"Unknown event type: {evs.Key}");
                }
            }
        }

        public static void Add<T>(this SourceBlockNotificationHooks<T> hooks, ConfigureHooks<T> c)
        {
            var newHooks = new SourceBlockNotificationHooks<T>(hooks.SourceBlock);
            c(newHooks);
            newHooks = newHooks.Build();

            var oldOnDeliveringMessages = hooks.OnDeliveringMessages;
            var oldOnReservationReleased = hooks.OnReservationReleased;
            var oldOnCompleting = hooks.OnCompleting;
            var oldCompletion = hooks.Completion;
            if (newHooks.OnDeliveringMessages != null)
            {
                hooks.OnDeliveringMessages = msg =>
                {
                    oldOnDeliveringMessages?.Invoke(msg);
                    newHooks.OnDeliveringMessages.Invoke(msg);
                };
            }
            if (newHooks.OnReservationReleased != null)
            {
                hooks.OnReservationReleased = msg =>
                {
                    oldOnReservationReleased?.Invoke(msg);
                    newHooks.OnReservationReleased.Invoke(msg);
                };
            }
            if (newHooks.OnCompleting != null)
            {
                hooks.OnCompleting = () =>
                {
                    oldOnCompleting?.Invoke();
                    newHooks.OnCompleting?.Invoke();
                };
            }
            hooks.Completion = Task.WhenAll(oldCompletion, newHooks.Completion);
        }
    }
}
