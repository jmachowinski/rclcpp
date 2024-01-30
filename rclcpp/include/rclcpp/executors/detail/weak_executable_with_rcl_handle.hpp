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
  WeakExecutableWithRclHandle(ExecutableRef executable,  RclHandle rcl_handle)
    : executable(std::move(executable)), rcl_handle(std::move(rcl_handle)), scheduler(nullptr), processed(false)
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

struct WeakExecutableWithRclHandleCache
{
  std::vector<AnyRef> timers;
  std::vector<AnyRef> subscribers;
  std::vector<AnyRef> clients;
  std::vector<AnyRef> services;
  std::vector<AnyRef> waitables;
  std::vector<AnyRef> guard_conditions;

  size_t temporaryCallbackGroupIndex = 0;

  bool cache_ditry = true;

  void regenerate(rclcpp::CallbackGroup & callback_group)
  {
    timers.clear();
    subscribers.clear();
    clients.clear();
    services.clear();
    waitables.clear();

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
      auto &entry = subscribers.emplace_back(SubscriberRef(s, std::move(handle_shr_ptr)));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };
    const auto add_timer = [this](const rclcpp::TimerBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_timer_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = timers.emplace_back(TimerRef(s, std::move(handle_shr_ptr)));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      const_cast<rcl_timer_t *>(handle_ptr)->user_data = &entry;
    };
    const auto add_client = [this](const rclcpp::ClientBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_client_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = clients.emplace_back(ClientRef(s, std::move(handle_shr_ptr)));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };
    const auto add_service = [this](const rclcpp::ServiceBase::SharedPtr &s)
    {
      auto handle_shr_ptr = s->get_service_handle();
      auto handle_ptr = handle_shr_ptr.get();
      auto &entry = services.emplace_back(ServiceRef(s, std::move(handle_shr_ptr)));
      // subscribers was reserved before hand, the pointer will not
      // change later on, therefore this is safe
      handle_ptr->user_data = &entry;
    };
    const auto add_waitable = [this](const rclcpp::Waitable::SharedPtr &s)
    {
      waitables.emplace_back(WaitableRef(s, s));
    };

    callback_group.collect_all_ptrs(add_sub, add_service, add_client, add_timer, add_waitable);

    guard_conditions.reserve(1);

    guard_conditions.emplace_back(GuardConditionWithFunction(callback_group.get_notify_guard_condition(), [this]() {
        cache_ditry = true;
      }
                                  ));

    cache_ditry = false;
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
    for(auto &any_ref: guard_conditions)
    {
      GuardConditionWithFunction &ref = std::get<GuardConditionWithFunction>(any_ref);
      add_to_wait_set(ws,  ref.guard_condition);
    }

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
    const size_t old_client_index = ws.client_index;
    const size_t old_event_index = ws.event_index;
    const size_t old_guard_condition_index = ws.guard_condition_index;
    const size_t old_service_index = ws.service_index;
    const size_t old_subscription_index = ws.subscription_index;
    const size_t old_timer_index = ws.timer_index;

    // reset the processed flag, used to make sure, that a waitable
    // will not get added two timers to the scheduler
    waitable.processed = false;
    waitable.rcl_handle->add_to_wait_set(&ws);

    {
      for (size_t i = old_client_index; i < ws.client_index; i++) {
        const_cast<rcl_client_t *>(ws.clients[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_event_index; i < ws.event_index; i++) {
        const_cast<rcl_event_t *>(ws.events[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_guard_condition_index; i < ws.guard_condition_index; i++) {
        const_cast<rcl_guard_condition_t *>(ws.guard_conditions[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_service_index; i < ws.service_index; i++) {
        const_cast<rcl_service_t *>(ws.services[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_subscription_index; i < ws.subscription_index; i++) {
        const_cast<rcl_subscription_t *>(ws.subscriptions[i])->user_data = &waitable;
      }
    }

    {
      for (size_t i = old_timer_index; i < ws.timer_index; i++) {
        const_cast<rcl_timer_t *>(ws.timers[i])->user_data = &waitable;
      }
    }

    return true;
  }

  void collect_ready_from_waitset(rcl_wait_set_s & ws);
};

}
