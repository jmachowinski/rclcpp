#pragma once
#include <functional>
#include <deque>
#include <chrono>
#include <rclcpp/timer.hpp>
#include <rclcpp/subscription_base.hpp>
#include <rclcpp/waitable.hpp>
#include <rclcpp/guard_condition.hpp>
#include <rclcpp/service.hpp>
#include <rclcpp/client.hpp>
#include <rclcpp/executor.hpp>

namespace rclcpp
{
namespace executors
{

struct AnyExecutableCbgEv
{
    std::function<void()> execute_function;

    bool more_executables_ready_in_cbg = false;
};

struct WaitableWithEventType
{
    rclcpp::Waitable::WeakPtr waitable;
    int internal_event_type;

    bool expired()
    {
        return waitable.expired();
    }
};

template<class ExecutableRef>
class ExecutionQueue
{
public:
    ExecutionQueue()
    {
    }

    void add_ready_executable(const ExecutableRef & e)
    {
        ready_executables.push_back(e);
    }

    bool has_unprocessed_executables()
    {
        while(!ready_executables.empty())
        {
            if(!ready_executables.front().expired())
            {
                return true;
            }
            ready_executables.pop_front();

        }

        return false;
    }

    bool get_unprocessed_executable(AnyExecutableCbgEv & any_executable)
    {
        while(!ready_executables.empty())
        {
            if (fill_any_executable(any_executable, ready_executables.front())) {
                ready_executables.pop_front();
                return true;
            }
            ready_executables.pop_front();
        }

        return false;
    }

    bool execute_unprocessed_executable_until(const std::chrono::time_point<std::chrono::steady_clock> &stop_time)
    {
        bool executed_executable = false;

        while(!ready_executables.empty())
        {
            AnyExecutableCbgEv any_executable;
            if (fill_any_executable(any_executable, ready_executables.front())) {
                ready_executables.pop_front();

                any_executable.execute_function();

                executed_executable = true;

                if(std::chrono::steady_clock::now() >= stop_time)
                {
                    return true;
                }
            }
        }

        return executed_executable;
    }


private:
    bool fill_any_executable(
        AnyExecutableCbgEv & any_executable,
        const rclcpp::SubscriptionBase::WeakPtr & ptr)
    {
        auto shr_ptr = ptr.lock();
        if(!shr_ptr)
        {
            return false;
        }
        any_executable.execute_function = [shr_ptr = std::move(shr_ptr)] () {
            rclcpp::Executor::execute_subscription(shr_ptr);
        };

        return true;
    }
    bool fill_any_executable(AnyExecutableCbgEv & any_executable, const rclcpp::TimerBase::WeakPtr & ptr)
    {
        auto shr_ptr = ptr.lock();
        if(!shr_ptr)
        {
            return false;
        }
        auto data = shr_ptr->call();
        if (!data) {
            // timer was cancelled, skip it.
            return false;
        }

        any_executable.execute_function = [shr_ptr = std::move(shr_ptr)] () {
            shr_ptr->execute_callback();
//       rclcpp::Executor::execute_timer(shr_ptr, data);
        };

        return true;
    }
    bool fill_any_executable(
        AnyExecutableCbgEv & any_executable,
        const rclcpp::ServiceBase::WeakPtr & ptr)
    {
        auto shr_ptr = ptr.lock();
        if(!shr_ptr)
        {
            return false;
        }
        any_executable.execute_function = [shr_ptr = std::move(shr_ptr)] () {
            rclcpp::Executor::execute_service(shr_ptr);
        };

        return true;
    }
    bool fill_any_executable(
        AnyExecutableCbgEv & any_executable,
        const rclcpp::ClientBase::WeakPtr & ptr)
    {
        auto shr_ptr = ptr.lock();
        if(!shr_ptr)
        {
            return false;
        }
        any_executable.execute_function = [shr_ptr = std::move(shr_ptr)] () {
            rclcpp::Executor::execute_client(shr_ptr);
        };

        return true;
    }
    bool fill_any_executable(
        AnyExecutableCbgEv & any_executable,
        const WaitableWithEventType & ev)
    {
        auto shr_ptr = ev.waitable.lock();
        if(!shr_ptr)
        {
            return false;
        }
        auto data = shr_ptr->take_data_by_entity_id(ev.internal_event_type);

        any_executable.execute_function = [shr_ptr = std::move(shr_ptr), data = std::move(data)] () {
            std::shared_ptr<void> cpy = std::move(data);
            shr_ptr->execute(cpy);
        };

        return true;
    }
    std::deque<ExecutableRef> ready_executables;

};


class CallbackGroupSchedulerEv
{
public:

    enum SchedulingPolicy
    {
        // Execute all ready events in priority order
        FistInFirstOut,
        // Only execute the highest ready event
        Prioritized,
    };

    CallbackGroupSchedulerEv(SchedulingPolicy sched_policy = SchedulingPolicy::Prioritized) :
        sched_policy(sched_policy)
    {
    }

    void clear_and_prepare(const size_t max_timer, const size_t max_subs, const size_t max_services, const size_t max_clients, const size_t max_waitables);

    void add_ready_executable(rclcpp::SubscriptionBase::WeakPtr & executable)
    {
        ready_subscriptions.add_ready_executable(executable);
    }
    void add_ready_executable(rclcpp::ServiceBase::WeakPtr & executable)
    {
        ready_services.add_ready_executable(executable);
    }
    void add_ready_executable(rclcpp::TimerBase::WeakPtr & executable)
    {
        ready_timers.add_ready_executable(executable);
    }
    void add_ready_executable(rclcpp::ClientBase::WeakPtr & executable)
    {
        ready_clients.add_ready_executable(executable);
    }
    void add_ready_executable(const WaitableWithEventType & executable)
    {
        ready_waitables.add_ready_executable(executable);
    }

    enum Priorities
    {
        Timer = 0,
        Subscription,
        Service,
        Client,
        Waitable
    };

    bool get_unprocessed_executable(AnyExecutableCbgEv & any_executable, enum Priorities for_priority)
    {
    switch (for_priority) {
    case Client:
        return ready_clients.get_unprocessed_executable(any_executable);
        break;
    case Service:
        return ready_services.get_unprocessed_executable(any_executable);
        break;
    case Subscription:
        return ready_subscriptions.get_unprocessed_executable(any_executable);
        break;
    case Timer:
        return ready_timers.get_unprocessed_executable(any_executable);
        break;
    case Waitable:
        return ready_waitables.get_unprocessed_executable(any_executable);
        break;
    }
    return false;
    }

    bool execute_unprocessed_executable_until(const std::chrono::time_point<std::chrono::steady_clock> &stop_time, enum Priorities for_priority)
    {
    switch (for_priority) {
    case Client:
        return ready_clients.execute_unprocessed_executable_until(stop_time);
        break;
    case Service:
        return ready_services.execute_unprocessed_executable_until(stop_time);
        break;
    case Subscription:
        return ready_subscriptions.execute_unprocessed_executable_until(stop_time);
        break;
    case Timer:
        return ready_timers.execute_unprocessed_executable_until(stop_time);
        break;
    case Waitable:
        return ready_waitables.execute_unprocessed_executable_until(stop_time);
        break;
    }
    return false;
    }

    bool has_unprocessed_executables()
    {
    return ready_subscriptions.has_unprocessed_executables() ||
           ready_timers.has_unprocessed_executables() ||
           ready_clients.has_unprocessed_executables() ||
           ready_services.has_unprocessed_executables() ||
           ready_waitables.has_unprocessed_executables();
    }

private:
    ExecutionQueue<rclcpp::TimerBase::WeakPtr> ready_timers;
    ExecutionQueue<rclcpp::SubscriptionBase::WeakPtr> ready_subscriptions;
    ExecutionQueue<rclcpp::ServiceBase::WeakPtr> ready_services;
    ExecutionQueue<rclcpp::ClientBase::WeakPtr> ready_clients;
    ExecutionQueue<WaitableWithEventType> ready_waitables;

    SchedulingPolicy sched_policy;
};

}
}
