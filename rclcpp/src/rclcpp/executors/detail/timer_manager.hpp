#pragma once
#include <chrono>
#include <functional>
#include <memory>
#include <vector>
#include <map>

#include <rcl/timer.h>
#include <rclcpp/timer.hpp>

namespace rclcpp::executors
{

class TimerQueue
{
    struct TimerData
    {
        std::shared_ptr<const rcl_timer_t> rcl_ref;
        std::function<void()> timer_ready_callback;
    };

public:
    TimerQueue(rcl_clock_type_t timer_type) :
        timer_type(timer_type),
        used_clock_for_timers(timer_type)
    {
    };

    void add_timer ( const rclcpp::TimerBase::SharedPtr &timer, const std::function<void()> &timer_ready_callback)
    {
        rcl_clock_t *clock_type_of_timer;

        std::shared_ptr<const rcl_timer_t> handle = timer->get_timer_handle();

        if(rcl_timer_clock(const_cast<rcl_timer_t *>(handle.get()), &clock_type_of_timer) != RCL_RET_OK)
        {
            assert(false);
        };

        if(clock_type_of_timer->type != timer_type)
        {
            // timer is handled by another queue
            return;
        }

        std::unique_ptr<TimerData> data = std::make_unique<TimerData>();
        data->timer_ready_callback = std::move(timer_ready_callback);
        data->rcl_ref = std::move(handle);

        timer->set_on_reset_callback ( [data_ptr = data.get(), this] ( size_t ) {
            add_timer_to_running_map(data_ptr);
        } );

        {
            std::scoped_lock l(mutex);
            add_timer_to_running_map(data.get());

            all_timers.emplace_back ( std::move(data) );
        }

        //wake up thread as new timer was added
        thread_conditional.notify_all();
    };

    void add_timer_to_running_map(const TimerData *timer_data)
    {
        if(timer_data->rcl_ref.unique())
        {
            // timer was deleted
            auto it = std::find_if(all_timers.begin(), all_timers.end(), [timer_data] (const std::unique_ptr<TimerData> &e) {
                return timer_data == e.get();
            }
            );

            if(it != all_timers.end())
            {
                all_timers.erase(it);
            }
            return;
        }

        int64_t next_call_time;

        auto ret = rcl_timer_get_next_call_time(timer_data->rcl_ref.get(), &next_call_time);

        if(ret == RCL_RET_OK)
        {
            running_timers.emplace(next_call_time, timer_data);
        }
    }

    /**
     * Returns the time when the next timer becomes ready
     */
    std::chrono::nanoseconds get_next_timer_ready_time() const
    {
        if(running_timers.empty())
        {
            return std::chrono::nanoseconds::max();
        }
        return running_timers.begin()->first;
    }

    void call_ready_timer_callbacks()
    {
        auto readd_timer_to_running_map = [this] (TimerMap::node_type &&e)
        {
            const auto &timer_data = e.mapped();
            if(timer_data->rcl_ref.unique())
            {
                // timer was deleted
                auto it = std::find_if(all_timers.begin(), all_timers.end(), [timer_data] (const std::unique_ptr<TimerData> &e) {
                    return timer_data == e.get();
                }
                );

                if(it != all_timers.end())
                {
                    all_timers.erase(it);
                }
                return;
            }

            int64_t next_call_time;

            auto ret = rcl_timer_get_next_call_time(timer_data->rcl_ref.get(), &next_call_time);

            if(ret == RCL_RET_OK)
            {
                e.key() = std::chrono::nanoseconds(next_call_time);
                running_timers.insert(std::move(e));
            }
        };

        while(!running_timers.empty())
        {
            int64_t time_until_call;

            auto ret = rcl_timer_get_time_until_next_call(running_timers.begin()->second->rcl_ref.get(), &time_until_call);
            if(ret == RCL_RET_TIMER_CANCELED)
            {
                running_timers.erase(running_timers.begin());
                continue;
            }

            if(time_until_call <= 0)
            {
                running_timers.begin()->second->timer_ready_callback();
                readd_timer_to_running_map(running_timers.extract(running_timers.begin()));
                continue;
            }
            break;
        }
    }

    void timer_thread()
    {
        while(running)
        {
            {
                std::scoped_lock l(mutex);

                call_ready_timer_callbacks();

                std::chrono::nanoseconds next_wakeup_time = get_next_timer_ready_time();

                used_clock_for_timers.sleep_until(rclcpp::Time(next_wakeup_time.count(), timer_type), thread_conditional);
            }
        }
    };

private:
    rcl_clock_type_t timer_type;
    rclcpp::Clock used_clock_for_timers;

    std::mutex mutex;

    bool running = true;

    std::vector<std::unique_ptr<TimerData>> all_timers;

    using TimerMap = std::multimap<std::chrono::nanoseconds, const TimerData *>;
    TimerMap running_timers;

    std::condition_variable thread_conditional;
};

class TimerManager
{
    std::array<TimerQueue, 3> timer_queues;
public:

    TimerManager() :
        timer_queues{RCL_ROS_TIME, RCL_SYSTEM_TIME, RCL_STEADY_TIME}
    {
    };

    void add_timer ( const rclcpp::TimerBase::SharedPtr &timer, const std::function<void()> &timer_ready_callback)
    {
        for(TimerQueue &q : timer_queues)
        {
            q.add_timer(timer, timer_ready_callback);
        }
    };
};
}
