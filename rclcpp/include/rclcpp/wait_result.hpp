// Copyright 2020 Open Source Robotics Foundation, Inc.
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

#ifndef RCLCPP__WAIT_RESULT_HPP_
#define RCLCPP__WAIT_RESULT_HPP_

#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>

#include "rcl/wait.h"

#include "rclcpp/macros.hpp"
#include "rclcpp/wait_result_kind.hpp"

#include "rclcpp/client.hpp"
#include "rclcpp/service.hpp"
#include "rclcpp/subscription_base.hpp"
#include "rclcpp/timer.hpp"

namespace rclcpp
{

// TODO(wjwwood): the union-like design of this class could be replaced with
//   std::variant, when we have access to that...
/// Interface for introspecting a wait set after waiting on it.
/**
 * This class:
 *
 *   - provides the result of waiting, i.e. ready, timeout, or empty, and
 *   - holds the ownership of the entities of the wait set, if needed, and
 *   - provides the necessary information for iterating over the wait set.
 *
 * This class is only valid as long as the wait set which created it is valid,
 * and it must be deleted before the wait set is deleted, as it contains a
 * back reference to the wait set.
 *
 * An instance of this, which is returned from rclcpp::WaitSetTemplate::wait(),
 * will cause the wait set to keep ownership of the entities because it only
 * holds a reference to the sequences of them, rather than taking a copy.
 * Also, in the thread-safe case, an instance of this will cause the wait set,
 * to block calls which modify the sequences of the entities, e.g. add/remove
 * guard condition or subscription methods.
 *
 * \tparam WaitSetT The wait set type which created this class.
 */
template<class WaitSetT>
class WaitResult final
{
public:
  /// Create WaitResult from a "ready" result.
  /**
   * \param[in] wait_set A reference to the wait set, which this class
   *   will keep for the duration of its lifetime.
   * \return a WaitResult from a "ready" result.
   */
  static
  WaitResult
  from_ready_wait_result_kind(WaitSetT & wait_set)
  {
    return WaitResult(WaitResultKind::Ready, wait_set);
  }

  /// Create WaitResult from a "timeout" result.
  static
  WaitResult
  from_timeout_wait_result_kind()
  {
    return WaitResult(WaitResultKind::Timeout);
  }

  /// Create WaitResult from a "empty" result.
  static
  WaitResult
  from_empty_wait_result_kind()
  {
    return WaitResult(WaitResultKind::Empty);
  }

  /// Return the kind of the WaitResult.
  WaitResultKind
  kind() const
  {
    return wait_result_kind_;
  }

  /// Return the rcl wait set.
  /**
   * \return const rcl wait set.
   * \throws std::runtime_error if the class cannot access wait set when the result was not ready
   */
  const WaitSetT &
  get_wait_set() const
  {
    if (this->kind() != WaitResultKind::Ready) {
      throw std::runtime_error("cannot access wait set when the result was not ready");
    }
    // This should never happen, defensive (and debug mode) check only.
    assert(wait_set_pointer_);
    return *wait_set_pointer_;
  }

  /// Return the rcl wait set.
  /**
   * \return rcl wait set.
   * \throws std::runtime_error if the class cannot access wait set when the result was not ready
   */
  WaitSetT &
  get_wait_set()
  {
    if (this->kind() != WaitResultKind::Ready) {
      throw std::runtime_error("cannot access wait set when the result was not ready");
    }
    // This should never happen, defensive (and debug mode) check only.
    assert(wait_set_pointer_);
    return *wait_set_pointer_;
  }

  WaitResult(WaitResult && other) noexcept
  : wait_result_kind_(other.wait_result_kind_),
    wait_set_pointer_(std::exchange(other.wait_set_pointer_, nullptr))
  {}

  ~WaitResult()
  {
    if (wait_set_pointer_) {
      wait_set_pointer_->wait_result_release();
    }
  }

  /// Get the next ready timer and its index in the wait result
  /**
   * If there is no ready timer, then nullptr will be returned and the index
   * will be invalid and should not be used.
   *
   * \param[in] start_index index at which to start searching for the next ready
   *   timer in the wait result. If the start_index is out of bounds for the
   *   list of timers in the wait result, then {nullptr, start_index} will be
   *   returned. Defaults to 0.
   * \return next ready timer pointer and its index in the wait result, or
   *   {nullptr, start_index} if none was found.
   */
  std::shared_ptr<rclcpp::TimerBase>
  next_ready_timer()
  {
    check_wait_result_dirty();
    auto ret = std::shared_ptr<rclcpp::TimerBase>{nullptr};
    if (this->kind() == WaitResultKind::Ready) {
      auto & wait_set = this->get_wait_set();
      auto & rcl_wait_set = wait_set.storage_get_rcl_wait_set();
      for (; next_timer_index_ < wait_set.size_of_timers(); ++next_timer_index_) {
        if (rcl_wait_set.timers[next_timer_index_] != nullptr) {
          ret = wait_set.timers(next_timer_index_);
          rcl_wait_set.timers[next_timer_index_] = nullptr;
          ++next_timer_index_;
          break;
        }
      }
    }
    return ret;
  }

  /// Get the next ready subscription, clearing it from the wait result.
  std::shared_ptr<rclcpp::SubscriptionBase>
  next_ready_subscription()
  {
    check_wait_result_dirty();
    auto ret = std::shared_ptr<rclcpp::SubscriptionBase>{nullptr};
    if (this->kind() == WaitResultKind::Ready) {
      auto & wait_set = this->get_wait_set();
      auto & rcl_wait_set = wait_set.storage_get_rcl_wait_set();
      for (; next_subscription_index_ < wait_set.size_of_subscriptions();
        ++next_subscription_index_)
      {
        if (rcl_wait_set.subscriptions[next_subscription_index_] != nullptr) {
          ret = wait_set.subscriptions(next_subscription_index_);
          rcl_wait_set.subscriptions[next_subscription_index_] = nullptr;
          ++next_subscription_index_;
          break;
        }
      }
    }
    return ret;
  }

  /// Get the next ready service, clearing it from the wait result.
  std::shared_ptr<rclcpp::ServiceBase>
  next_ready_service()
  {
    check_wait_result_dirty();
    auto ret = std::shared_ptr<rclcpp::ServiceBase>{nullptr};
    if (this->kind() == WaitResultKind::Ready) {
      auto & wait_set = this->get_wait_set();
      auto & rcl_wait_set = wait_set.storage_get_rcl_wait_set();
      for (; next_service_index_ < wait_set.size_of_services(); ++next_service_index_) {
        if (rcl_wait_set.services[next_service_index_] != nullptr) {
          ret = wait_set.services(next_service_index_);
          rcl_wait_set.services[next_service_index_] = nullptr;
          ++next_service_index_;
          break;
        }
      }
    }
    return ret;
  }

  /// Get the next ready client, clearing it from the wait result.
  std::shared_ptr<rclcpp::ClientBase>
  next_ready_client()
  {
    check_wait_result_dirty();
    auto ret = std::shared_ptr<rclcpp::ClientBase>{nullptr};
    if (this->kind() == WaitResultKind::Ready) {
      auto & wait_set = this->get_wait_set();
      auto & rcl_wait_set = wait_set.storage_get_rcl_wait_set();
      for (; next_client_index_ < wait_set.size_of_clients(); ++next_client_index_) {
        if (rcl_wait_set.clients[next_client_index_] != nullptr) {
          ret = wait_set.clients(next_client_index_);
          rcl_wait_set.clients[next_client_index_] = nullptr;
          ++next_client_index_;
          break;
        }
      }
    }
    return ret;
  }

  /// Get the next ready waitable, clearing it from the wait result.
  std::shared_ptr<rclcpp::Waitable>
  next_ready_waitable()
  {
    check_wait_result_dirty();
    auto waitable = std::shared_ptr<rclcpp::Waitable>{nullptr};
    auto data = std::shared_ptr<void>{nullptr};

    if (this->kind() == WaitResultKind::Ready) {
      auto & wait_set = this->get_wait_set();
      auto rcl_wait_set = wait_set.get_rcl_wait_set();
      for (; next_waitable_index_ < wait_set.size_of_waitables(); ++next_waitable_index_) {
        auto cur_waitable = wait_set.waitables(next_waitable_index_);
        if (cur_waitable != nullptr && cur_waitable->is_ready(rcl_wait_set)) {
          waitable = cur_waitable;
          ++next_waitable_index_;
          break;
        }
      }
    }

    return waitable;
  }

private:
  RCLCPP_DISABLE_COPY(WaitResult)

  explicit WaitResult(WaitResultKind wait_result_kind)
  : wait_result_kind_(wait_result_kind)
  {
    // Should be enforced by the static factory methods on this class.
    assert(WaitResultKind::Ready != wait_result_kind);
  }

  WaitResult(WaitResultKind wait_result_kind, WaitSetT & wait_set)
  : wait_result_kind_(wait_result_kind),
    wait_set_pointer_(&wait_set)
  {
    // Should be enforced by the static factory methods on this class.
    assert(WaitResultKind::Ready == wait_result_kind);
    // Secure thread-safety (if provided) and shared ownership (if needed).
    this->get_wait_set().wait_result_acquire();
  }

  /// Check if the wait result is invalid because the wait set was modified.
  void
  check_wait_result_dirty()
  {
    // In the case that the wait set was modified while the result was out,
    // we must mark the wait result as no longer valid
    if (wait_set_pointer_ && this->get_wait_set().wait_result_dirty_) {
      this->wait_result_kind_ = WaitResultKind::Invalid;
    }
  }

  WaitResultKind wait_result_kind_;

  WaitSetT * wait_set_pointer_ = nullptr;

  size_t next_timer_index_ = 0;
  size_t next_subscription_index_ = 0;
  size_t next_service_index_ = 0;
  size_t next_client_index_ = 0;
  size_t next_waitable_index_ = 0;
};

}  // namespace rclcpp

#endif  // RCLCPP__WAIT_RESULT_HPP_
