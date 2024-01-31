#pragma once
#include "rclcpp/subscription_base.hpp"
#include "rclcpp/timer.hpp"
#include "rclcpp/service.hpp"
#include "rclcpp/client.hpp"
#include "rclcpp/waitable.hpp"
#include "rclcpp/guard_condition.hpp"
#include "rclcpp/callback_group.hpp"

namespace rclcpp::executors
{

class CallbackGroupScheduler;

template <class ExecutableRef, class RclHandle>
struct WeakExecutableWithRclHandle
{
  WeakExecutableWithRclHandle(ExecutableRef executable,  RclHandle rcl_handle, CallbackGroupScheduler *scheduler)
    : executable(std::move(executable)), rcl_handle(std::move(rcl_handle)), scheduler(scheduler), processed(false)
  {
  }



  ExecutableRef executable;
  RclHandle rcl_handle;
  CallbackGroupScheduler *scheduler;
  bool processed;

  bool executable_alive()
  {
    auto use_cnt = rcl_handle.use_count();
    if(use_cnt == 0)
    {
      return false;
    }

    if(use_cnt <= 1)
    {
      executable.reset();
      rcl_handle.reset();
      return false;
    }

    return true;
  }

};

struct GuardConditionWithFunction
{
  GuardConditionWithFunction(rclcpp::GuardCondition::SharedPtr gc, std::function<void(void)> fun) :
    guard_condition(std::move(gc)), handle_guard_condition_fun(std::move(fun))
  {
  }

  rclcpp::GuardCondition::SharedPtr guard_condition;

  // A function that should be executed if the guard_condition is ready
  std::function<void(void)> handle_guard_condition_fun;
};

using TimerRef = WeakExecutableWithRclHandle<rclcpp::TimerBase::WeakPtr,std::shared_ptr<const rcl_timer_t>>;

using SubscriberRef = WeakExecutableWithRclHandle<rclcpp::SubscriptionBase::WeakPtr,std::shared_ptr<rcl_subscription_t>>;
using ClientRef = WeakExecutableWithRclHandle<rclcpp::ClientBase::WeakPtr,std::shared_ptr<rcl_client_t>>;
using ServiceRef = WeakExecutableWithRclHandle<rclcpp::ServiceBase::WeakPtr,std::shared_ptr<rcl_service_t>>;
using WaitableRef = WeakExecutableWithRclHandle<rclcpp::Waitable::WeakPtr, rclcpp::Waitable::SharedPtr>;

using AnyRef = std::variant<TimerRef, SubscriberRef, ClientRef, ServiceRef, WaitableRef, GuardConditionWithFunction>;

/// Helper class to compute the size of a waitset
struct WaitSetSize
{
  size_t subscriptions = 0;
  size_t clients = 0;
  size_t services = 0;
  size_t timers = 0;
  size_t guard_conditions = 0;
  size_t events = 0;

  void clear()
  {
    subscriptions = 0;
  clients = 0;
  services = 0;
  timers = 0;
  guard_conditions = 0;
  events = 0;
  }

  void addWaitable(const rclcpp::Waitable::SharedPtr & waitable_ptr)
  {
      if (!waitable_ptr) {
        return;
      }

      subscriptions += waitable_ptr->get_number_of_ready_subscriptions();
      clients += waitable_ptr->get_number_of_ready_clients();
      services += waitable_ptr->get_number_of_ready_services();
      timers += waitable_ptr->get_number_of_ready_timers();
      guard_conditions += waitable_ptr->get_number_of_ready_guard_conditions();
      events += waitable_ptr->get_number_of_ready_events();
  }

  void add(const WaitSetSize &waitset)
  {
    subscriptions += waitset.subscriptions;
    clients += waitset.clients;
    services += waitset.services;
    timers += waitset.timers;
    guard_conditions += waitset.guard_conditions;
    events += waitset.events;
//     RCUTILS_LOG_ERROR_NAMED("rclcpp", "add: waitset new size : t %lu, s %lu, c %lu, s %lu, gc %lu", timers, subscriptions, clients, services, guard_conditions);
  }

  void clear_and_resize_wait_set(rcl_wait_set_s & wait_set) const
  {
    // clear wait set
    rcl_ret_t ret = rcl_wait_set_clear(&wait_set);
    if (ret != RCL_RET_OK) {
      exceptions::throw_from_rcl_error(ret, "Couldn't clear wait set");
    }

//     RCUTILS_LOG_ERROR_NAMED("rclcpp", "Needed size of waitset : t %lu, s %lu, c %lu, s %lu, gc %lu", timers, subscriptions, clients, services, guard_conditions);
    if(wait_set.size_of_subscriptions < subscriptions || wait_set.size_of_guard_conditions < guard_conditions ||
      wait_set.size_of_timers < timers || wait_set.size_of_clients < clients || wait_set.size_of_services < services || wait_set.size_of_events < events)
    {
      // The size of waitables are accounted for in size of the other entities
      ret = rcl_wait_set_resize(
        &wait_set, subscriptions,
        guard_conditions, timers,
        clients, services, events);
      if (RCL_RET_OK != ret) {
        exceptions::throw_from_rcl_error(ret, "Couldn't resize the wait set");
      }
    }
  }
};


struct WeakExecutableWithRclHandleCache
{
  WeakExecutableWithRclHandleCache(CallbackGroupScheduler &scheduler);
  WeakExecutableWithRclHandleCache();

  std::vector<AnyRef> timers;
  std::vector<AnyRef> subscribers;
  std::vector<AnyRef> clients;
  std::vector<AnyRef> services;
  std::vector<AnyRef> waitables;
  std::vector<AnyRef> guard_conditions;

  CallbackGroupScheduler &scheduler;

  bool cache_ditry = true;

  WaitSetSize wait_set_size;

  void regenerate(rclcpp::CallbackGroup & callback_group)
  {
    timers.clear();
    subscribers.clear();
    clients.clear();
    services.clear();
    waitables.clear();
    guard_conditions.clear();

    wait_set_size.clear();

    // we reserve to much memory here, this this should be fine
    timers.reserve(callback_group.size());
    subscribers.reserve(callback_group.size());
    clients.reserve(callback_group.size());
    services.reserve(callback_group.size());
    waitables.reserve(callback_group.size());

    const auto add_sub =     [this](const rclcpp::SubscriptionBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_subscription_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = subscribers.emplace_back(SubscriberRef(s, std::move(handle_shr_ptr), &scheduler));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };
    const auto add_timer = [this](const rclcpp::TimerBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_timer_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = timers.emplace_back(TimerRef(s, std::move(handle_shr_ptr), &scheduler));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      const_cast<rcl_timer_t *>(handle_ptr)->user_data = &entry;
    };
    const auto add_client = [this](const rclcpp::ClientBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_client_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = clients.emplace_back(ClientRef(s, std::move(handle_shr_ptr), &scheduler));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };
    const auto add_service = [this](const rclcpp::ServiceBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_service_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = services.emplace_back(ServiceRef(s, std::move(handle_shr_ptr), &scheduler));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };

    wait_set_size.guard_conditions = 0;

    guard_conditions.reserve(1);

    add_guard_condition(callback_group.get_notify_guard_condition(), [this]() {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "GC: Callback group was changed");

        cache_ditry = true;
      });

    const auto add_waitable = [this](const rclcpp::Waitable::SharedPtr &s)
    {
      waitables.emplace_back(WaitableRef(s, s, &scheduler));
      wait_set_size.addWaitable(s);
    };

    callback_group.collect_all_ptrs(add_sub, add_service, add_client, add_timer, add_waitable);

    wait_set_size.timers += timers.size();
    wait_set_size.subscriptions += subscribers.size();
    wait_set_size.clients += clients.size();
    wait_set_size.services += services.size();

    //RCUTILS_LOG_ERROR_NAMED("rclcpp", "GC: regeneraetd, new size : t %lu, s %lu, c %lu, s %lu, gc %lu", wait_set_size.timers, wait_set_size.subscriptions, wait_set_size.clients, wait_set_size.services, wait_set_size.guard_conditions);


    cache_ditry = false;
  }

  void add_guard_condition(rclcpp::GuardCondition::SharedPtr ptr, std::function<void(void)> fun)
  {
    guard_conditions.emplace_back(GuardConditionWithFunction(std::move(ptr), std::move(fun)));

    for(auto &entry : guard_conditions)
    {
      GuardConditionWithFunction &gcwf = std::get<GuardConditionWithFunction>(entry);
      gcwf.guard_condition->get_rcl_guard_condition().user_data = &entry;
    }

    wait_set_size.guard_conditions++;
  }

  template <class RefType>
  bool add_container_to_wait_set(std::vector<AnyRef> &container, rcl_wait_set_s & ws)
  {
    for(auto &any_ref: container)
    {
      RefType &ref = std::get<RefType>(any_ref);
      if(ref.executable_alive())
      {
        if(!add_to_wait_set(ws,  ref.rcl_handle.get()))
        {
          return false;
        }
      }
    }

    return true;
  }

  bool add_to_wait_set(rcl_wait_set_s & ws)
  {
    if(!add_container_to_wait_set<TimerRef>(timers, ws))
    {
      return false;
    }
    if(!add_container_to_wait_set<SubscriberRef>(subscribers, ws))
    {
      return false;
    }
    if(!add_container_to_wait_set<ServiceRef>(services, ws))
    {
      return false;
    }
    if(!add_container_to_wait_set<ClientRef>(clients, ws))
    {
      return false;
    }
    for(auto &any_ref: waitables)
    {
      WaitableRef &ref = std::get<WaitableRef>(any_ref);
      if(ref.executable_alive())
      {
        add_to_wait_set(ws,  ref);
      }
    }

    //RCUTILS_LOG_ERROR_NAMED("rclcpp", "Adding %lu guard conditions", guard_conditions.size());

    for(auto &any_ref: guard_conditions)
    {
      GuardConditionWithFunction &ref = std::get<GuardConditionWithFunction>(any_ref);
      add_to_wait_set(ws,  ref.guard_condition);
    }

    return true;
  }

  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    rcl_subscription_t *handle_ptr) const
  {
    if (rcl_wait_set_add_subscription(
        &ws, handle_ptr,
        nullptr) != RCL_RET_OK)
    {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add subscription to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    return true;
  }

  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    const rcl_timer_t *handle_ptr) const
  {
    if (rcl_wait_set_add_timer(&ws, handle_ptr, nullptr) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add timer to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    return true;
  }
  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    const rcl_service_t *handle_ptr) const
  {
    if (rcl_wait_set_add_service(&ws, handle_ptr, nullptr) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add service to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    return true;
  }
  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    const rclcpp::GuardCondition::SharedPtr &gc_ptr)
  {
    rcl_ret_t ret = rcl_wait_set_add_guard_condition(&ws, &(gc_ptr->get_rcl_guard_condition()), nullptr);

    if (RCL_RET_OK != ret) {
      rclcpp::exceptions::throw_from_rcl_error(
        ret, "failed to add guard condition to wait set");
    }

    return true;
  }


  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    const rcl_client_t *handle_ptr) const
  {
    if (rcl_wait_set_add_client(&ws, handle_ptr, nullptr) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add client to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    return true;
  }
  bool add_to_wait_set(
    rcl_wait_set_s & ws,
    WaitableRef &waitable) const
  {
    const size_t nr_of_added_clients = ws.nr_of_added_clients;
    const size_t nr_of_added_events = ws.nr_of_added_events;
    const size_t old_nr_of_added_guard_conditions = ws.nr_of_added_guard_conditions;
    const size_t old_nr_of_added_services = ws.nr_of_added_services;
    const size_t old_nr_of_added_subscriptions = ws.nr_of_added_subscriptions;
    const size_t old_nr_of_added_timers = ws.nr_of_added_timers;

    // reset the processed flag, used to make sure, that a waitable
    // will not get added two timers to the scheduler
    waitable.processed = false;
    waitable.rcl_handle->add_to_wait_set(&ws);

    {
      for (size_t i = nr_of_added_clients; i < ws.nr_of_added_clients; i++) {
        const_cast<rcl_client_t *>(ws.clients[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = nr_of_added_events; i < ws.nr_of_added_events; i++) {
        const_cast<rcl_event_t *>(ws.events[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_nr_of_added_guard_conditions; i < ws.nr_of_added_guard_conditions; i++) {
        const_cast<rcl_guard_condition_t *>(ws.guard_conditions[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_nr_of_added_services; i < ws.nr_of_added_services; i++) {
        const_cast<rcl_service_t *>(ws.services[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_nr_of_added_subscriptions; i < ws.nr_of_added_subscriptions; i++) {
        const_cast<rcl_subscription_t *>(ws.subscriptions[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_nr_of_added_timers; i < ws.nr_of_added_timers; i++) {
        const_cast<rcl_timer_t *>(ws.timers[i])->user_data = &waitable;
      }
    }

    return true;
  }

  static void collect_ready_from_waitset(rcl_wait_set_s & ws);
};



}
