// Copyright 2024 Cellumation GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rclcpp/executors/cbg_executor.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

#include "rcpputils/scope_exit.hpp"
#include "rclcpp/exceptions/exceptions.hpp"
#include "rclcpp/detail/add_guard_condition_to_rcl_wait_set.hpp"

#include "rclcpp/logging.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/utilities.hpp"

namespace rclcpp::executors
{

struct AnyExecutableWeakRef
{
  AnyExecutableWeakRef(const rclcpp::SubscriptionBase::WeakPtr & p, int16_t callback_group_index)
  : executable(p),
    callback_group_index(callback_group_index)
  {}

  AnyExecutableWeakRef(const rclcpp::TimerBase::WeakPtr & p, int16_t callback_group_index)
  : executable(p),
    callback_group_index(callback_group_index)
  {}

  AnyExecutableWeakRef(const rclcpp::ServiceBase::WeakPtr & p, int16_t callback_group_index)
  : executable(p),
    callback_group_index(callback_group_index)
  {}

  AnyExecutableWeakRef(const rclcpp::ClientBase::WeakPtr & p, int16_t callback_group_index)
  : executable(p),
    callback_group_index(callback_group_index)
  {}

  AnyExecutableWeakRef(const rclcpp::Waitable::WeakPtr & p, int16_t callback_group_index)
  : executable(p),
    callback_group_index(callback_group_index)
  {}

  AnyExecutableWeakRef(
    const rclcpp::GuardCondition::WeakPtr & p,
    const std::function<void(void)> & fun)
  : executable(p),
    handle_guard_condition_fun(fun),
    callback_group_index(-1),
    processed(!fun)
  {
    //special case, guard conditions are auto processed by waking up the wait set
    // therefore they shall never create a real executable
  }

  std::variant<const rclcpp::SubscriptionBase::WeakPtr, const rclcpp::TimerBase::WeakPtr,
    const rclcpp::ServiceBase::WeakPtr, const rclcpp::ClientBase::WeakPtr,
    const rclcpp::Waitable::WeakPtr, const rclcpp::GuardCondition::WeakPtr> executable;
  std::function<void(void)> handle_guard_condition_fun;
  int16_t callback_group_index;
  bool processed = false;
};


struct WaitSetSize
{
  size_t subscriptions = 0;
  size_t clients = 0;
  size_t services = 0;
  size_t timers = 0;
  size_t guard_conditions = 0;
  size_t events = 0;

  void addCallbackGroupState(const CallbackGroupState & state)
  {
    subscriptions += state.subscription_ptrs.size();
    clients += state.client_ptrs.size();
    services += state.service_ptrs.size();
    timers += state.timer_ptrs.size();
    // A callback group contains one guard condition
    guard_conditions++;

    for (const rclcpp::Waitable::WeakPtr & waitable_weak_ptr: state.waitable_ptrs) {
      rclcpp::Waitable::SharedPtr waitable_ptr = waitable_weak_ptr.lock();
      if (!waitable_ptr) {
        continue;
      }

      subscriptions += waitable_ptr->get_number_of_ready_subscriptions();
      clients += waitable_ptr->get_number_of_ready_clients();
      services += waitable_ptr->get_number_of_ready_services();
      timers += waitable_ptr->get_number_of_ready_timers();
      guard_conditions += waitable_ptr->get_number_of_ready_guard_conditions();
      events += waitable_ptr->get_number_of_ready_events();
    }
  }

  void add_guard_condition()
  {
    guard_conditions++;
  }

  void clear_and_resize_wait_set(rcl_wait_set_s & wait_set) const
  {
    // clear wait set
    rcl_ret_t ret = rcl_wait_set_clear(&wait_set);
    if (ret != RCL_RET_OK) {
      exceptions::throw_from_rcl_error(ret, "Couldn't clear wait set");
    }

    // The size of waitables are accounted for in size of the other entities
    ret = rcl_wait_set_resize(
      &wait_set, subscriptions,
      guard_conditions, timers,
      clients, services, events);
    if (RCL_RET_OK != ret) {
      exceptions::throw_from_rcl_error(ret, "Couldn't resize the wait set");
    }
  }
};

/// Just a collection of AnyExecutableWeakRef of a callback group
struct ExecutableWeakPtrCache
{
  std::vector<AnyExecutableWeakRef> executables;

  size_t temporaryCallbackGroupIndex = 0;

  bool cache_ditry = true;

  void regenerate(const CallbackGroupState & state)
  {
    executables.clear();
    executables.reserve(
      state.client_ptrs.size() +
      state.service_ptrs.size() +
      state.subscription_ptrs.size() +
      state.timer_ptrs.size() +
      state.waitable_ptrs.size() + 1);

    for (const rclcpp::SubscriptionBase::WeakPtr & weak_ptr: state.subscription_ptrs) {
      executables.emplace_back(weak_ptr, temporaryCallbackGroupIndex);
    }

    for (const rclcpp::ClientBase::WeakPtr & weak_ptr: state.client_ptrs) {
      executables.emplace_back(weak_ptr, temporaryCallbackGroupIndex);
    }

    for (const rclcpp::ServiceBase::WeakPtr & weak_ptr: state.service_ptrs) {
      executables.emplace_back(weak_ptr, temporaryCallbackGroupIndex);
    }

    for (const rclcpp::TimerBase::WeakPtr & weak_ptr: state.timer_ptrs) {
      executables.emplace_back(weak_ptr, temporaryCallbackGroupIndex);
    }

    for (const rclcpp::Waitable::WeakPtr & weak_ptr: state.waitable_ptrs) {
      executables.emplace_back(weak_ptr, temporaryCallbackGroupIndex);
    }

    // FIXME insert regenerate trigger function here
    executables.emplace_back(
      state.trigger_ptr, [this]() {
        cache_ditry = true;
      }
    );

    cache_ditry = false;
  }

};

struct RCLToRCLCPPMap
{
  RCLToRCLCPPMap(const WaitSetSize & wait_set_size)
  {
    subscription_map.reserve(wait_set_size.subscriptions);
    guard_conditions_map.reserve(wait_set_size.guard_conditions);
    timer_map.reserve(wait_set_size.timers);
    clients_map.reserve(wait_set_size.clients);
    services_map.reserve(wait_set_size.services);
    events_map.reserve(wait_set_size.events);
  }

  std::vector<AnyExecutableWeakRef *> subscription_map;
  std::vector<AnyExecutableWeakRef *> guard_conditions_map;
  std::vector<AnyExecutableWeakRef *> timer_map;
  std::vector<AnyExecutableWeakRef *> clients_map;
  std::vector<AnyExecutableWeakRef *> services_map;
  std::vector<AnyExecutableWeakRef *> events_map;

  bool add_to_wait_set_and_mapping(rcl_wait_set_s & ws, AnyExecutableWeakRef & executable_ref)
  {
    switch (executable_ref.executable.index()) {
      case 0:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::SubscriptionBase::WeakPtr>(
              executable_ref.executable), executable_ref);
        }
        break;
      case 1:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::TimerBase::WeakPtr>(executable_ref.executable), executable_ref);
        }
        break;
      case 2:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::ServiceBase::WeakPtr>(executable_ref.executable),
            executable_ref);
        }
        break;
      case 3:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::ClientBase::WeakPtr>(executable_ref.executable), executable_ref);
        }
        break;
      case 4:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::Waitable::WeakPtr>(executable_ref.executable), executable_ref);
        }
        break;
      case 5:
        {
          return add_to_wait_set_and_mapping(
            ws,
            std::get<const rclcpp::GuardCondition::WeakPtr>(
              executable_ref.executable), executable_ref);
        }
        break;

    }

    return false;
  }


  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws,
    const rclcpp::SubscriptionBase::WeakPtr & sub_weak_ptr,
    AnyExecutableWeakRef & executable_ref)
  {
    const rclcpp::SubscriptionBase::SharedPtr & sub_ptr = sub_weak_ptr.lock();
    if (!sub_ptr) {
      // got deleted, we just ignore it
      return true;
    }

    subscription_map.push_back(&executable_ref);

    size_t idx;

    if (rcl_wait_set_add_subscription(
        &ws, sub_ptr->get_subscription_handle().get(),
        &idx) != RCL_RET_OK)
    {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add subscription to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    // verify that our mapping is correct
    assert(idx == subscription_map.size() - 1);

    return true;

  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws,
    const rclcpp::ClientBase::WeakPtr & client_weak_ptr,
    AnyExecutableWeakRef & any_exec)
  {
    const rclcpp::ClientBase::SharedPtr & client_ptr = client_weak_ptr.lock();
    if (!client_ptr) {
      // got deleted, we just ignore it from now on
      return true;
    }

    clients_map.push_back(&any_exec);

    size_t idx;

    if (rcl_wait_set_add_client(&ws, client_ptr->get_client_handle().get(), &idx) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add client to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    // verify that our mapping is correct
    assert(idx == clients_map.size() - 1);
    return true;
  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws,
    const rclcpp::ServiceBase::WeakPtr & weak_ptr,
    AnyExecutableWeakRef & any_exec)
  {
    const rclcpp::ServiceBase::SharedPtr & shr_ptr = weak_ptr.lock();
    if (!shr_ptr) {
      // got deleted, we just ignore it from now on
      return true;
    }

    services_map.push_back(&any_exec);

    size_t idx;

    if (rcl_wait_set_add_service(&ws, shr_ptr->get_service_handle().get(), &idx) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add service to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    // verify that our mapping is correct
    assert(idx == services_map.size() - 1);
    return true;
  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws, const rclcpp::TimerBase::WeakPtr & weak_ptr,
    AnyExecutableWeakRef & any_exec)
  {
    const rclcpp::TimerBase::SharedPtr & shr_ptr = weak_ptr.lock();
    if (!shr_ptr) {
      // got deleted, we just ignore it from now on
      return true;
    }

    timer_map.push_back(&any_exec);

    size_t idx;

    if (rcl_wait_set_add_timer(&ws, shr_ptr->get_timer_handle().get(), &idx) != RCL_RET_OK) {
      RCUTILS_LOG_ERROR_NAMED(
        "rclcpp",
        "Couldn't add timer to wait set: %s", rcl_get_error_string().str);
      return false;
    }

    // verify that our mapping is correct
    assert(idx == timer_map.size() - 1);
    return true;
  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws, const rclcpp::Waitable::WeakPtr & weak_ptr,
    AnyExecutableWeakRef & any_exec)
  {
    const rclcpp::Waitable::SharedPtr & waitable_ptr = weak_ptr.lock();
    if (!waitable_ptr) {
      // got deleted, we just ignore it from now on
      return true;
    }

    rcl_wait_set_indices_t before_waitable;
    rcl_get_wait_set_indices(&ws, &before_waitable);

    waitable_ptr->add_to_wait_set(&ws);

    rcl_wait_set_indices_t after_waitable;
    rcl_get_wait_set_indices(&ws, &after_waitable);

//         RCUTILS_LOG_ERROR_NAMED(
//             "rclcpp",
//             ("CBGExecutor::add_to_wait_set_and_mapping() : Adding waitalbe, ws.size_of_events : " + std::to_string(after_waitable.event_index) + " before_waitable.size_of_events " + std::to_string(before_waitable.event_index)).c_str());

    {
      const size_t diff = after_waitable.client_index - before_waitable.client_index;
      for (size_t i = 0; i < diff; i++) {
        clients_map.push_back(&any_exec);
      }
    }

    {
      const size_t diff = after_waitable.event_index - before_waitable.event_index;
      for (size_t i = 0; i < diff; i++) {
        events_map.push_back(&any_exec);
      }
    }

    {
      const size_t diff = after_waitable.guard_condition_index -
        before_waitable.guard_condition_index;
      for (size_t i = 0; i < diff; i++) {
        guard_conditions_map.push_back(&any_exec);
      }
    }

    {
      const size_t diff = after_waitable.service_index - before_waitable.service_index;
      for (size_t i = 0; i < diff; i++) {
        services_map.push_back(&any_exec);
      }
    }

    {
      const size_t diff = after_waitable.subscription_index - before_waitable.subscription_index;
      for (size_t i = 0; i < diff; i++) {
        subscription_map.push_back(&any_exec);
      }
    }

    {
      const size_t diff = after_waitable.timer_index - before_waitable.timer_index;
      for (size_t i = 0; i < diff; i++) {
        timer_map.push_back(&any_exec);
      }
    }

    return true;
  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws,
    const rclcpp::GuardCondition::WeakPtr & weak_ptr,
    AnyExecutableWeakRef & any_exec)
  {
//         RCUTILS_LOG_ERROR_NAMED(
//             "rclcpp",
//             ("CBGExecutor::add_to_wait_set_and_mapping() : Adding GuardCondition, ws.size_of_events : " + std::to_string(ws.size_of_events)).c_str());


    rclcpp::GuardCondition::SharedPtr shr_ptr = weak_ptr.lock();

    if (!shr_ptr) {
      return false;
    }

    const auto & gc = shr_ptr->get_rcl_guard_condition();

    size_t idx = 0;
    rcl_ret_t ret = rcl_wait_set_add_guard_condition(&ws, &gc, &idx);

    if (RCL_RET_OK != ret) {
      rclcpp::exceptions::throw_from_rcl_error(
        ret, "failed to add guard condition to wait set");
    }

    guard_conditions_map.push_back(&any_exec);

    return true;
  }

  bool add_to_wait_set_and_mapping(
    rcl_wait_set_s & ws, ExecutableWeakPtrCache & exec_cache,
    int16_t callback_group_idx)
  {
    for (AnyExecutableWeakRef & ref: exec_cache.executables) {
      ref.callback_group_index = callback_group_idx;
      ref.processed = false;

      if (!add_to_wait_set_and_mapping(ws, ref)) {
        return false;
      }
    }

    return true;
  }

};


CBGExecutor::CBGExecutor(
  const rclcpp::ExecutorOptions & options,
  size_t number_of_threads,
  std::chrono::nanoseconds next_exec_timeout)
: next_exec_timeout_(next_exec_timeout),

  spinning(false),
  interrupt_guard_condition_(std::make_shared<rclcpp::GuardCondition>(options.context)),
  shutdown_guard_condition_(std::make_shared<rclcpp::GuardCondition>(options.context)),
  context_(options.context),
  global_executable_cache(std::make_unique<ExecutableWeakPtrCache>())
{

  global_executable_cache->executables.emplace_back(
    interrupt_guard_condition_,
    std::function<void(void)>());
  global_executable_cache->executables.emplace_back(
    shutdown_guard_condition_,
    std::function<void(void)>());


  number_of_threads_ = number_of_threads > 0 ?
    number_of_threads :
    std::max(std::thread::hardware_concurrency(), 2U);

  shutdown_callback_handle_ = context_->add_on_shutdown_callback(
    [weak_gc = std::weak_ptr<rclcpp::GuardCondition> {shutdown_guard_condition_}]() {
      auto strong_gc = weak_gc.lock();
      if (strong_gc) {
        strong_gc->trigger();
      }
    });

  rcl_allocator_t allocator = options.memory_strategy->get_allocator();

  rcl_ret_t ret = rcl_wait_set_init(
    &wait_set_,
    0, 2, 0, 0, 0, 0,
    context_->get_rcl_context().get(),
    allocator);
  if (RCL_RET_OK != ret) {
    RCUTILS_LOG_ERROR_NAMED(
      "rclcpp",
      "failed to create wait set: %s", rcl_get_error_string().str);
    rcl_reset_error();
    exceptions::throw_from_rcl_error(ret, "Failed to create wait set in Executor constructor");
  }

}

CBGExecutor::~CBGExecutor()
{

  std::vector<node_interfaces::NodeBaseInterface::WeakPtr> added_nodes_cpy;
  {
    std::lock_guard lock{added_nodes_mutex_};
    added_nodes_cpy = added_nodes;
  }

  for (const node_interfaces::NodeBaseInterface::WeakPtr & node_weak_ptr : added_nodes_cpy) {
    const node_interfaces::NodeBaseInterface::SharedPtr & node_ptr = node_weak_ptr.lock();
    if (node_ptr) {
      remove_node(node_ptr, false);
    }
  }

  std::vector<rclcpp::CallbackGroup::WeakPtr> added_cbgs_cpy;
  {
    std::lock_guard lock{added_callback_groups_mutex_};
    added_cbgs_cpy = added_callback_groups;
  }

  for (const auto & weak_ptr : added_cbgs_cpy) {
    auto shr_ptr = weak_ptr.lock();
    if (shr_ptr) {
      remove_callback_group(shr_ptr, false);
    }
  }

  // Remove shutdown callback handle registered to Context
  if (!context_->remove_on_shutdown_callback(shutdown_callback_handle_)) {
    RCUTILS_LOG_ERROR_NAMED(
      "rclcpp",
      "failed to remove registered on_shutdown callback");
    rcl_reset_error();
  }

}

void CallbackGroupScheduler::prepare(const CallbackGroupState & cb_elements)
{
  ready_timers.clear_and_prepare(cb_elements.timer_ptrs.size());
  ready_subscriptions.clear_and_prepare(cb_elements.subscription_ptrs.size());
  ready_services.clear_and_prepare(cb_elements.service_ptrs.size());
  ready_clients.clear_and_prepare(cb_elements.client_ptrs.size());
  ready_waitables.clear_and_prepare(cb_elements.waitable_ptrs.size());
}

bool CallbackGroupScheduler::get_unprocessed_executable(
  AnyExecutable & any_executable,
  enum Priorities for_priority)
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

bool CallbackGroupScheduler::has_unprocessed_executables()
{
  return ready_clients.has_unprocessed_executables() ||
         ready_services.has_unprocessed_executables() ||
         ready_subscriptions.has_unprocessed_executables() ||
         ready_timers.has_unprocessed_executables() ||
         ready_waitables.has_unprocessed_executables();
}

void CallbackGroupScheduler::add_ready_executable(
  const rclcpp::SubscriptionBase::WeakPtr & executable)
{
  ready_subscriptions.add_ready_executable(executable);
}
void CallbackGroupScheduler::add_ready_executable(const rclcpp::ServiceBase::WeakPtr & executable)
{
  ready_services.add_ready_executable(executable);
}
void CallbackGroupScheduler::add_ready_executable(const rclcpp::TimerBase::WeakPtr & executable)
{
  ready_timers.add_ready_executable(executable);
}
void CallbackGroupScheduler::add_ready_executable(const rclcpp::ClientBase::WeakPtr & executable)
{
  ready_clients.add_ready_executable(executable);
}
void CallbackGroupScheduler::add_ready_executable(const rclcpp::Waitable::WeakPtr & executable)
{
  ready_waitables.add_ready_executable(executable);
}


bool CBGExecutor::get_next_ready_executable(AnyExecutable & any_executable)
{
  struct ReadyCallbacksWithSharedPtr
  {
    CallbackGroupData * data;
    rclcpp::CallbackGroup::SharedPtr callback_group;
  };

  std::vector<ReadyCallbacksWithSharedPtr> ready_callbacks;
  ready_callbacks.reserve(callback_groups.size());


  for (auto it = callback_groups.begin(); it != callback_groups.end(); ) {
    CallbackGroupData & cbg_with_data(*it);

    ReadyCallbacksWithSharedPtr e;
    e.callback_group = cbg_with_data.callback_group.lock();
    if (!e.callback_group) {
      it = callback_groups.erase(it);
      continue;
    }

    if (e.callback_group->can_be_taken_from().load()) {
      e.data = &cbg_with_data;
      ready_callbacks.push_back(std::move(e));
    }

    it++;
  }

  for (size_t i = CallbackGroupScheduler::Priorities::Timer;
    i <= CallbackGroupScheduler::Priorities::Waitable; i++)
  {
    CallbackGroupScheduler::Priorities cur_prio(static_cast<CallbackGroupScheduler::Priorities>(i));
    for (const ReadyCallbacksWithSharedPtr & ready_elem: ready_callbacks) {
      if (ready_elem.data->scheduler->get_unprocessed_executable(any_executable, cur_prio)) {
        any_executable.callback_group = ready_elem.callback_group;
        if (any_executable.waitable) {
          any_executable.data = any_executable.waitable->take_data();
        }

        return true;
      }
    }
  }

  return false;
}

size_t
CBGExecutor::get_number_of_threads()
{
  return number_of_threads_;
}

void CBGExecutor::sync_callback_groups()
{
  if (!needs_callback_group_resync.exchange(false)) {
    return;
  }

  std::vector<std::pair<CallbackGroupData *, rclcpp::CallbackGroup::SharedPtr>> cur_group_data;
  cur_group_data.reserve(callback_groups.size());

  for (CallbackGroupData & d : callback_groups) {
    auto p = d.callback_group.lock();
    if (p) {
      cur_group_data.emplace_back(&d, std::move(p));
    }
  }

  std::vector<CallbackGroupData> next_group_data;

  auto insert_data = [&cur_group_data, &next_group_data](rclcpp::CallbackGroup::SharedPtr & cbg)
    {
      for (const auto & pair : cur_group_data) {
        if (pair.second == cbg) {
//                 RCUTILS_LOG_INFO("Using existing callback group");
          next_group_data.push_back(std::move(*pair.first));
          return;
        }
      }

//         RCUTILS_LOG_INFO("Using new callback group");

      CallbackGroupData new_entry;
      new_entry.callback_group = cbg;
      new_entry.scheduler = std::make_unique<CallbackGroupScheduler>();
      new_entry.callback_group_state = std::make_unique<CallbackGroupState>(*cbg);
      new_entry.executable_cache = std::make_unique<ExecutableWeakPtrCache>();
      new_entry.executable_cache->regenerate(*new_entry.callback_group_state);
      new_entry.callback_group_state_needs_update = false;
      next_group_data.push_back(std::move(new_entry));
    };

  {
    std::vector<rclcpp::CallbackGroup::WeakPtr> added_cbgs_cpy;
    {
      std::lock_guard lock{added_callback_groups_mutex_};
      added_cbgs_cpy = added_callback_groups;
    }

    std::vector<node_interfaces::NodeBaseInterface::WeakPtr> added_nodes_cpy;
    {
      std::lock_guard lock{added_nodes_mutex_};
      added_nodes_cpy = added_nodes;
    }

    // *3 ist a good estimate of how many callback_group a node may have
    next_group_data.reserve(added_cbgs_cpy.size() + added_nodes_cpy.size() * 3);

    for (const node_interfaces::NodeBaseInterface::WeakPtr & node_weak_ptr : added_nodes_cpy) {
      auto node_ptr = node_weak_ptr.lock();
      if (node_ptr) {
        node_ptr->for_each_callback_group(
          [&insert_data](rclcpp::CallbackGroup::SharedPtr cbg)
          {
            if (cbg->automatically_add_to_executor_with_node()) {
              insert_data(cbg);
            }
          });
      }
    }

    for (const rclcpp::CallbackGroup::WeakPtr & cbg : added_cbgs_cpy) {
      auto p = cbg.lock();
      if (p) {
        insert_data(p);
      }
    }
  }

  callback_groups.swap(next_group_data);
}

void CBGExecutor::wait_for_work(
  std::chrono::nanoseconds timeout,
  bool do_not_wait_if_all_groups_busy)
{
  using namespace rclcpp::exceptions;

  WaitSetSize wait_set_size;

  sync_callback_groups();

  std::vector<CallbackGroupData *> idle_callback_groups;
  idle_callback_groups.reserve(callback_groups.size());

  for (CallbackGroupData & cbg_with_data: callback_groups) {
    if (cbg_with_data.scheduler->has_unprocessed_executables()) {
      continue;
    }

    auto cbg_shr_ptr = cbg_with_data.callback_group.lock();
    if (!cbg_shr_ptr) {
      continue;
    }

    if (!cbg_shr_ptr->can_be_taken_from()) {
      continue;
    }

    CallbackGroupState & cbg_state = *cbg_with_data.callback_group_state;
    // regenerate the state data
    if (cbg_with_data.executable_cache->cache_ditry ||
      cbg_with_data.callback_group_state_needs_update)
    {
//             RCUTILS_LOG_INFO("Regenerating callback group");
      cbg_state.update(*cbg_shr_ptr);
      cbg_with_data.executable_cache->regenerate(cbg_state);
      cbg_with_data.callback_group_state_needs_update = false;
      cbg_with_data.executable_cache->cache_ditry = false;
    }

    wait_set_size.addCallbackGroupState(cbg_state);

    idle_callback_groups.push_back(&cbg_with_data);
  }

  if (do_not_wait_if_all_groups_busy && idle_callback_groups.empty()) {
    return;
  }

  // interrupt_guard_condition_ and shutdown_guard_condition_
  wait_set_size.add_guard_condition();
  wait_set_size.add_guard_condition();


  //FIXME add a guard condition per node

  // init the mapping with the known size of all involved objects
  RCLToRCLCPPMap mapping(wait_set_size);

  // prepare the wait set
  wait_set_size.clear_and_resize_wait_set(wait_set_);


  //FIXME add a guard condition per node

  mapping.add_to_wait_set_and_mapping(wait_set_, *global_executable_cache, -1);

  // add all ready callback groups
  for (size_t idx = 0; idx < idle_callback_groups.size(); idx++) {
    CallbackGroupData * cbg_with_data = idle_callback_groups[idx];
    const CallbackGroupState & callback_group_state(*cbg_with_data->callback_group_state);

    mapping.add_to_wait_set_and_mapping(
      wait_set_, *cbg_with_data->executable_cache,
      static_cast<int16_t>(idx));

    // setup the groups for the next round
    cbg_with_data->scheduler->prepare(callback_group_state);
  }

  rcl_ret_t status =
    rcl_wait(&wait_set_, std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count());
  if (status == RCL_RET_WAIT_SET_EMPTY) {
    RCUTILS_LOG_WARN_NAMED(
      "rclcpp",
      "empty wait set received in rcl_wait(). This should never happen.");
  } else if (status != RCL_RET_OK && status != RCL_RET_TIMEOUT) {
    using rclcpp::exceptions::throw_from_rcl_error;
    throw_from_rcl_error(status, "rcl_wait() failed");
  }

  fill_callback_group_data(wait_set_, idle_callback_groups, mapping);

  //at this point we don't need the wait_set_ any more
}

void CBGExecutor::fill_callback_group_data(
  rcl_wait_set_s & wait_set,
  const std::vector<CallbackGroupData *> idle_callback_groups, const RCLToRCLCPPMap & mapping)
{
  auto add_executable = [&wait_set, &idle_callback_groups](AnyExecutableWeakRef & ready_exec)
    {
      if (ready_exec.processed) {
        return;
      }

      switch (ready_exec.executable.index()) {
        case 0:
          {
            idle_callback_groups[ready_exec.callback_group_index]->scheduler->add_ready_executable(
              std::get<const rclcpp::SubscriptionBase::WeakPtr>(
                ready_exec.executable));
          }
          break;
        case 1:
          {
            idle_callback_groups[ready_exec.callback_group_index]->scheduler->add_ready_executable(
              std::get<const rclcpp::TimerBase::WeakPtr>(
                ready_exec.executable));
          }
          break;
        case 2:
          {
            idle_callback_groups[ready_exec.callback_group_index]->scheduler->add_ready_executable(
              std::get<const rclcpp::ServiceBase::WeakPtr>(
                ready_exec.executable));
          }
          break;
        case 3:
          {
            idle_callback_groups[ready_exec.callback_group_index]->scheduler->add_ready_executable(
              std::get<const rclcpp::ClientBase::WeakPtr>(
                ready_exec.executable));
          }
          break;
        case 4:
          {
            const rclcpp::Waitable::WeakPtr & waitable_weak =
              std::get<const rclcpp::Waitable::WeakPtr>(ready_exec.executable);
            rclcpp::Waitable::SharedPtr waitable = waitable_weak.lock();
            if (waitable && waitable->is_ready(&wait_set)) {
              idle_callback_groups[ready_exec.callback_group_index]->scheduler->add_ready_executable(
                waitable_weak);
            }
          }
          break;
        case 5:
          {
            if (ready_exec.handle_guard_condition_fun) {
              // one of our internal guard conditions triggered, lets execute the function callback for it
              ready_exec.handle_guard_condition_fun();
            }
          }
          break;

      }

      ready_exec.processed = true;
    };

  for (size_t i = 0; i < wait_set.size_of_clients; ++i) {
    if (wait_set.clients[i]) {
      //RCUTILS_LOG_I("Found ready client");
      AnyExecutableWeakRef & ready_exec(*mapping.clients_map[i]);
      add_executable(ready_exec);
    }
  }
  for (size_t i = 0; i < wait_set.size_of_events; ++i) {
    if (wait_set.events[i]) {
      //RCUTILS_LOG_I("Found ready events");
      AnyExecutableWeakRef & ready_exec(*mapping.events_map[i]);
      add_executable(ready_exec);
    }
  }
  for (size_t i = 0; i < wait_set.size_of_guard_conditions; ++i) {
    if (wait_set.guard_conditions[i]) {
      //RCUTILS_LOG_I("Found ready guard_conditions");
      AnyExecutableWeakRef & ready_exec(*mapping.guard_conditions_map[i]);
      add_executable(ready_exec);
    }
  }
  for (size_t i = 0; i < wait_set.size_of_services; ++i) {
    if (wait_set.services[i]) {
      //RCUTILS_LOG_I("Found ready services");
      AnyExecutableWeakRef & ready_exec(*mapping.services_map[i]);
      add_executable(ready_exec);
    }
  }
  for (size_t i = 0; i < wait_set.size_of_subscriptions; ++i) {
    if (wait_set.subscriptions[i]) {
      //RCUTILS_LOG_I("Found ready subscriptions");
      AnyExecutableWeakRef & ready_exec(*mapping.subscription_map[i]);
      add_executable(ready_exec);
    }
  }
  for (size_t i = 0; i < wait_set.size_of_timers; ++i) {
    if (wait_set.timers[i]) {
      //RCUTILS_LOG_I("Found ready timers");
      AnyExecutableWeakRef & ready_exec(*mapping.timer_map[i]);
      add_executable(ready_exec);
    }
  }
}

void
CBGExecutor::run(size_t this_thread_number)
{
  (void)this_thread_number;
  while (rclcpp::ok(this->context_) && spinning.load()) {
    spin_once_internal(next_exec_timeout_);
  }
}

void CBGExecutor::spin_once_internal(std::chrono::nanoseconds timeout)
{
  rclcpp::AnyExecutable any_exec;
  {
    std::lock_guard wait_lock{wait_mutex_};

//         RCUTILS_LOG_ERROR_NAMED(
//             "rclcpp",
//             "CBGExecutor::spin_once_internal()");


    if (!rclcpp::ok(this->context_) || !spinning.load()) {
      return;
    }

    if (!get_next_ready_executable(any_exec)) {
      /*
                  RCUTILS_LOG_ERROR_NAMED(
                      "rclcpp",
                      "CBGExecutor::spin_once_internal() : No work ready, waiting");*/

      wait_for_work(timeout);
      /*
                  RCUTILS_LOG_ERROR_NAMED(
                      "rclcpp",
                      "CBGExecutor::spin_once_internal() : Wait done");*/

      if (!get_next_ready_executable(any_exec)) {
//                 RCUTILS_LOG_ERROR_NAMED(
//                     "rclcpp",
//                     "CBGExecutor::spin_once_internal() : No work ready");
        return;
      }
    }
  }

//     RCUTILS_LOG_ERROR_NAMED(
//         "rclcpp",
//         "CBGExecutor::spin_once_internal() : Execute !");

  execute_any_executable(any_exec);

  // Clear the callback_group to prevent the AnyExecutable destructor from
  // resetting the callback group `can_be_taken_from`
  any_exec.callback_group.reset();
}

void
CBGExecutor::spin_once(std::chrono::nanoseconds timeout)
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("spin_once() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );

  spin_once_internal(timeout);
}

void
CBGExecutor::execute_any_executable(AnyExecutable & any_exec)
{
  if (!spinning.load()) {
    return;
  }
  if (any_exec.timer) {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "Executing Timer");


    TRACETOOLS_TRACEPOINT(
      rclcpp_executor_execute,
      static_cast<const void *>(any_exec.timer->get_timer_handle().get()));
    rclcpp::Executor::execute_timer(any_exec.timer);
  }
  if (any_exec.subscription) {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "Executing subscription");
    TRACETOOLS_TRACEPOINT(
      rclcpp_executor_execute,
      static_cast<const void *>(any_exec.subscription->get_subscription_handle().get()));
    rclcpp::Executor::execute_subscription(any_exec.subscription);
  }
  if (any_exec.service) {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "Executing service");
    rclcpp::Executor::execute_service(any_exec.service);
  }
  if (any_exec.client) {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "Executing client");
    rclcpp::Executor::execute_client(any_exec.client);
  }
  if (any_exec.waitable) {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "Executing waitable");
    any_exec.waitable->execute(any_exec.data);
  }
  // Reset the callback_group, regardless of type
  any_exec.callback_group->can_be_taken_from().store(true);
  // Wake the wait, because it may need to be recalculated or work that
  // was previously blocked is now available.
  try {
    interrupt_guard_condition_->trigger();
  } catch (const rclcpp::exceptions::RCLError & ex) {
    throw std::runtime_error(
            std::string(
              "Failed to trigger guard condition from execute_any_executable: ") + ex.what());
  }

}

bool CBGExecutor::collect_and_execute_ready_events(
  std::chrono::nanoseconds max_duration,
  bool recollect_if_no_work_available)
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("collect_and_execute_ready_events() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );

  auto start = std::chrono::steady_clock::now();
  auto cur_time = start;

  // collect any work, that is already ready
  wait_for_work(std::chrono::nanoseconds::zero(), true);

  bool work_available = false;
  bool got_work_since_collect = false;

  bool had_work = false;

  while (spinning && cur_time - start <= max_duration) {
    rclcpp::AnyExecutable any_exec;
    {
      std::lock_guard wait_lock{wait_mutex_};

//             RCUTILS_LOG_ERROR_NAMED(
//                 "rclcpp",
//                 "CBGExecutor::collect_and_execute_ready_events()");


      if (!rclcpp::ok(this->context_) || !spinning.load()) {
        return false;
      }

      if (!get_next_ready_executable(any_exec)) {

        work_available = false;
      } else {
        work_available = true;
        got_work_since_collect = true;
      }
    }

    if (!work_available) {
      if (!recollect_if_no_work_available) {
        // we are done
        return had_work;
      }

      if (got_work_since_collect) {
        // collect any work, that is already ready
        wait_for_work(std::chrono::nanoseconds::zero(), true);

        got_work_since_collect = false;
        continue;
      }

      return had_work;
    }

    /*
            RCUTILS_LOG_ERROR_NAMED(
                "rclcpp",
                "CBGExecutor::collect_and_execute_ready_events() : Execute !");*/

    execute_any_executable(any_exec);

    had_work = true;

    // Clear the callback_group to prevent the AnyExecutable destructor from
    // resetting the callback group `can_be_taken_from`
    any_exec.callback_group.reset();

    cur_time = std::chrono::steady_clock::now();
  }

  return had_work;
}

void
CBGExecutor::spin_some(std::chrono::nanoseconds max_duration)
{
  collect_and_execute_ready_events(max_duration, false);
}

void CBGExecutor::spin_all(std::chrono::nanoseconds max_duration)
{
  if (max_duration < std::chrono::nanoseconds::zero()) {
    throw std::invalid_argument("max_duration must be greater than or equal to 0");
  }

  collect_and_execute_ready_events(max_duration, true);
}

void
CBGExecutor::spin()
{
//     RCUTILS_LOG_ERROR_NAMED(
//         "rclcpp",
//         "CBGExecutor::spin()");


  if (spinning.exchange(true)) {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(this->spinning.store(false); );
  std::vector<std::thread> threads;
  size_t thread_id = 0;
  {
    std::lock_guard wait_lock{wait_mutex_};
    for (; thread_id < number_of_threads_ - 1; ++thread_id) {
      auto func = std::bind(&CBGExecutor::run, this, thread_id);
      threads.emplace_back(func);
    }
  }

  run(thread_id);
  for (auto & thread : threads) {
    thread.join();
  }
}

void
CBGExecutor::add_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
  bool notify)
{
  {
    std::lock_guard lock{added_callback_groups_mutex_};
    added_callback_groups.push_back(group_ptr);
  }
  needs_callback_group_resync = true;
  if (notify) {
    // Interrupt waiting to handle new node
    try {
      interrupt_guard_condition_->trigger();
    } catch (const rclcpp::exceptions::RCLError & ex) {
      throw std::runtime_error(
              std::string(
                "Failed to trigger guard condition on callback group add: ") + ex.what());
    }
  }
}

void
CBGExecutor::cancel()
{
  spinning.store(false);
  try {
    interrupt_guard_condition_->trigger();
  } catch (const rclcpp::exceptions::RCLError & ex) {
    throw std::runtime_error(
            std::string("Failed to trigger guard condition in cancel: ") + ex.what());
  }
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
CBGExecutor::get_all_callback_groups()
{
  std::lock_guard lock{added_callback_groups_mutex_};
  return added_callback_groups;
}

void
CBGExecutor::remove_callback_group(
  rclcpp::CallbackGroup::SharedPtr group_ptr,
  bool notify)
{
  bool found = false;
  {
    std::lock_guard lock{added_callback_groups_mutex_};
    added_callback_groups.erase(
      std::remove_if(
        added_callback_groups.begin(), added_callback_groups.end(),
        [&group_ptr, &found](const auto & weak_ptr)
        {
          auto shr_ptr = weak_ptr.lock();
          if (!shr_ptr) {
            return true;
          }

          if (group_ptr == shr_ptr) {
            found = true;
            return true;
          }
          return false;
        }), added_callback_groups.end());
    added_callback_groups.push_back(group_ptr);
  }

  if (found) {
    needs_callback_group_resync = true;
  }

  if (notify) {
    // Interrupt waiting to handle new node
    try {
      interrupt_guard_condition_->trigger();
    } catch (const rclcpp::exceptions::RCLError & ex) {
      throw std::runtime_error(
              std::string(
                "Failed to trigger guard condition on callback group add: ") + ex.what());
    }
  }
}

void
CBGExecutor::add_node(rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify)
{
  // If the node already has an executor
  std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
  if (has_executor.exchange(true)) {
    throw std::runtime_error(
            std::string("Node '") + node_ptr->get_fully_qualified_name() +
            "' has already been added to an executor.");
  }

  {
    std::lock_guard lock{added_nodes_mutex_};
    added_nodes.push_back(node_ptr);
  }

  needs_callback_group_resync = true;

  if (notify) {
    // Interrupt waiting to handle new node
    try {
      interrupt_guard_condition_->trigger();
    } catch (const rclcpp::exceptions::RCLError & ex) {
      throw std::runtime_error(
              std::string(
                "Failed to trigger guard condition on callback group add: ") + ex.what());
    }
  }
}

void
CBGExecutor::add_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  add_node(node_ptr->get_node_base_interface(), notify);
}

void
CBGExecutor::remove_node(
  rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
  bool notify)
{
  {
    std::lock_guard lock{added_nodes_mutex_};
    added_nodes.erase(
      std::remove_if(
        added_nodes.begin(), added_nodes.end(), [&node_ptr](const auto & weak_ptr) {
          const auto shr_ptr = weak_ptr.lock();
          if (shr_ptr && shr_ptr == node_ptr) {
            return true;
          }
          return false;
        }), added_nodes.end());
  }

  needs_callback_group_resync = true;

  if (notify) {
    // Interrupt waiting to handle new node
    try {
      interrupt_guard_condition_->trigger();
    } catch (const rclcpp::exceptions::RCLError & ex) {
      throw std::runtime_error(
              std::string(
                "Failed to trigger guard condition on callback group add: ") + ex.what());
    }
  }

  node_ptr->get_associated_with_executor_atomic().store(false);
}

void
CBGExecutor::remove_node(std::shared_ptr<rclcpp::Node> node_ptr, bool notify)
{
  remove_node(node_ptr->get_node_base_interface(), notify);
}

// add a callback group to the executor, not bound to any node
void CBGExecutor::add_callback_group_only(rclcpp::CallbackGroup::SharedPtr group_ptr)
{
  add_callback_group(group_ptr, nullptr, true);
}
}
