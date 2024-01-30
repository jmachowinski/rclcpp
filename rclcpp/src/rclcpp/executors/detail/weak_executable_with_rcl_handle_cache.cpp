#include <rclcpp/executors/detail/weak_executable_with_rcl_handle.hpp>
#include <rclcpp/executors/cbg_executor.hpp>
namespace rclcpp::executors
{

template <class RefType, class RclType>
inline void add_to_scheduler(const RclType *entry, rcl_wait_set_s & ws)
{
    if(entry)
    {
        AnyRef *any_ref = static_cast<AnyRef *>(entry->user_data);

        if(TimerRef *tim_ptr = std::get_if<TimerRef>(any_ref))
        {
            tim_ptr->scheduler->add_ready_executable(*tim_ptr);
        }
        else
        {
            WaitableRef &ref = std::get<WaitableRef>(*any_ref);

            if(ref.processed)
            {
                return;
            }

            ref.processed = true;

            if(ref.rcl_handle->is_ready(&ws))
            {
                ref.scheduler->add_ready_executable(ref);
            }
        }
    }

}

void WeakExecutableWithRclHandleCache::collect_ready_from_waitset(rcl_wait_set_s & ws)
{
    for (size_t i = 0; i < ws.timer_index; i++) {
        add_to_scheduler<TimerRef>(ws.timers[i], ws);
    }
    for (size_t i = 0; i < ws.subscription_index; i++) {
        add_to_scheduler<SubscriberRef>(ws.timers[i], ws);
    }
    for (size_t i = 0; i < ws.client_index; i++) {
        add_to_scheduler<ClientRef>(ws.timers[i], ws);
    }
    for (size_t i = 0; i < ws.service_index; i++) {
        add_to_scheduler<ServiceRef>(ws.timers[i], ws);
    }
    for (size_t i = 0; i < ws.event_index; i++) {
        add_to_scheduler<WaitableRef>(ws.timers[i], ws);
    }
    //FIXME collect guard condition functions
}
}
