// Copyright 2017 Open Source Robotics Foundation, Inc.
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

/**
 * This test checks all implementations of rclcpp::executor to check they pass they basic API
 * tests. Anything specific to any executor in particular should go in a separate test file.
 *
 */
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rcl/error_handling.h"
#include "rcl/time.h"
#include "rclcpp/clock.hpp"
#include "rclcpp/detail/add_guard_condition_to_rcl_wait_set.hpp"
#include "rclcpp/duration.hpp"
#include "rclcpp/guard_condition.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/time_source.hpp"

#include "test_msgs/msg/empty.hpp"
#include "rosgraph_msgs/msg/clock.hpp"
#include "rclcpp/executors/cbg_executor.hpp"
#include "test_msgs/srv/empty.hpp"

using namespace std::chrono_literals;

template<typename T>
class TestExecutors : public ::testing::Test
{
public:
  void SetUp()
  {
    rclcpp::init(0, nullptr);

    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::stringstream test_name;
    test_name << test_info->test_case_name() << "_" << test_info->name();
    node = std::make_shared<rclcpp::Node>("node", test_name.str());

    callback_count = 0;

    const std::string topic_name = std::string("topic_") + test_name.str();
    publisher = node->create_publisher<test_msgs::msg::Empty>(topic_name, rclcpp::QoS(10));
    auto callback = [this](test_msgs::msg::Empty::ConstSharedPtr) {this->callback_count++;};
    subscription =
      node->create_subscription<test_msgs::msg::Empty>(
      topic_name, rclcpp::QoS(10), std::move(callback));
  }

  void TearDown()
  {
    publisher.reset();
    subscription.reset();
    node.reset();

    rclcpp::shutdown();
  }

  rclcpp::Node::SharedPtr node;
  rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr publisher;
  rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr subscription;
  int callback_count;
};

// spin_all and spin_some are not implemented correctly in StaticSingleThreadedExecutor, see:
// https://github.com/ros2/rclcpp/issues/1219 for tracking
template<typename T>
class TestExecutorsStable : public TestExecutors<T> {};

using ExecutorTypes =
  ::testing::Types<
  rclcpp::executors::SingleThreadedExecutor,
  rclcpp::executors::MultiThreadedExecutor,
  rclcpp::executors::StaticSingleThreadedExecutor,
  rclcpp::executors::CBGExecutor,
  rclcpp::experimental::executors::EventsExecutor>;

class ExecutorTypeNames
{
public:
  template<typename T>
  static std::string GetName(int idx)
  {
    (void)idx;
    if (std::is_same<T, rclcpp::executors::SingleThreadedExecutor>()) {
      return "SingleThreadedExecutor";
    }

    if (std::is_same<T, rclcpp::executors::MultiThreadedExecutor>()) {
      return "MultiThreadedExecutor";
    }

    if (std::is_same<T, rclcpp::executors::StaticSingleThreadedExecutor>()) {
      return "StaticSingleThreadedExecutor";
    }

    if (std::is_same<T, rclcpp::executors::CBGExecutor>()) {
      return "CallbackGroupExecutor";
    }

    if (std::is_same<T, rclcpp::experimental::executors::EventsExecutor>()) {
      return "EventsExecutor";
    }

    return "";
  }
};

// TYPED_TEST_SUITE is deprecated as of gtest 1.9, use TYPED_TEST_SUITE when gtest dependency
// is updated.
TYPED_TEST_SUITE(TestExecutors, ExecutorTypes, ExecutorTypeNames);

// StaticSingleThreadedExecutor is not included in these tests for now, due to:
// https://github.com/ros2/rclcpp/issues/1219
using StandardExecutors =
  ::testing::Types<
  rclcpp::executors::SingleThreadedExecutor,
  rclcpp::executors::MultiThreadedExecutor,
  rclcpp::executors::CBGExecutor,
  rclcpp::experimental::executors::EventsExecutor>;
TYPED_TEST_SUITE(TestExecutorsStable, StandardExecutors, ExecutorTypeNames);

// Make sure that executors detach from nodes when destructing
TYPED_TEST(TestExecutors, detachOnDestruction)
{
  using ExecutorType = TypeParam;
  {
    ExecutorType executor;
    executor.add_node(this->node);
  }
  {
    ExecutorType executor;
    EXPECT_NO_THROW(executor.add_node(this->node));
  }
}

// Make sure that the executor can automatically remove expired nodes correctly
// Currently fails for StaticSingleThreadedExecutor so it is being skipped, see:
// https://github.com/ros2/rclcpp/issues/1231
TYPED_TEST(TestExecutorsStable, addTemporaryNode)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;

  {
    // Let node go out of scope before executor.spin()
    auto node = std::make_shared<rclcpp::Node>("temporary_node");
    executor.add_node(node);
  }

  // Sleep for a short time to verify executor.spin() is going, and didn't throw.
  std::thread spinner([&]() {EXPECT_NO_THROW(executor.spin());});

  std::this_thread::sleep_for(50ms);
  executor.cancel();
  spinner.join();
}

// Make sure that a spinning empty executor can be cancelled
TYPED_TEST(TestExecutors, emptyExecutor)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  std::thread spinner([&]() {EXPECT_NO_THROW(executor.spin());});
  std::this_thread::sleep_for(50ms);
  executor.cancel();
  spinner.join();
}

// Check executor throws properly if the same node is added a second time
TYPED_TEST(TestExecutors, addNodeTwoExecutors)
{
  using ExecutorType = TypeParam;
  ExecutorType executor1;
  ExecutorType executor2;
  EXPECT_NO_THROW(executor1.add_node(this->node));
  EXPECT_THROW(executor2.add_node(this->node), std::runtime_error);
  executor1.remove_node(this->node, true);
}

// Check simple spin example
TYPED_TEST(TestExecutors, spinWithTimer)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;

  bool timer_completed = false;
  auto timer = this->node->create_wall_timer(1ms, [&]() {timer_completed = true;});
  executor.add_node(this->node);

  std::thread spinner([&]() {executor.spin();});

  auto start = std::chrono::steady_clock::now();
  while (!timer_completed && (std::chrono::steady_clock::now() - start) < 10s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(timer_completed);
  // Cancel needs to be called before join, so that executor.spin() returns.
  executor.cancel();
  spinner.join();
  executor.remove_node(this->node, true);
}

TYPED_TEST(TestExecutors, spinWhileAlreadySpinning)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  bool timer_completed = false;
  auto timer = this->node->create_wall_timer(1ms, [&]() {timer_completed = true;});

  std::thread spinner([&]() {executor.spin();});
  // Sleep for a short time to verify executor.spin() is going, and didn't throw.

  auto start = std::chrono::steady_clock::now();
  while (!timer_completed && (std::chrono::steady_clock::now() - start) < 10s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(timer_completed);
  EXPECT_THROW(executor.spin(), std::runtime_error);

  // Shutdown needs to be called before join, so that executor.spin() returns.
  executor.cancel();
  spinner.join();
  executor.remove_node(this->node, true);
}

// Check executor exits immediately if future is complete.
TYPED_TEST(TestExecutors, testSpinUntilFutureComplete)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  // test success of an immediately finishing future
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);

  // spin_until_future_complete is expected to exit immediately, but would block up until its
  // timeout if the future is not checked before spin_once_impl.
  auto start = std::chrono::steady_clock::now();
  auto shared_future = future.share();
  auto ret = executor.spin_until_future_complete(shared_future, 1s);
  executor.remove_node(this->node, true);
  // Check it didn't reach timeout
  EXPECT_GT(500ms, (std::chrono::steady_clock::now() - start));
  EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
}

// Same test, but uses a shared future.
TYPED_TEST(TestExecutors, testSpinUntilSharedFutureComplete)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  // test success of an immediately finishing future
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);

  // spin_until_future_complete is expected to exit immediately, but would block up until its
  // timeout if the future is not checked before spin_once_impl.
  auto shared_future = future.share();
  auto start = std::chrono::steady_clock::now();
  auto ret = executor.spin_until_future_complete(shared_future, 1s);
  executor.remove_node(this->node, true);

  // Check it didn't reach timeout
  EXPECT_GT(500ms, (std::chrono::steady_clock::now() - start));
  EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
}

// For a longer running future that should require several iterations of spin_once
TYPED_TEST(TestExecutors, testSpinUntilFutureCompleteNoTimeout)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  // This future doesn't immediately terminate, so some work gets performed.
  std::future<void> future = std::async(
    std::launch::async,
    [this]() {
      auto start = std::chrono::steady_clock::now();
      while (this->callback_count < 1 && (std::chrono::steady_clock::now() - start) < 1s) {
        std::this_thread::sleep_for(1ms);
      }
    });

  bool spin_exited = false;

  // Timeout set to negative for no timeout.
  std::thread spinner([&]() {
      auto ret = executor.spin_until_future_complete(future, -1s);
      EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
      executor.remove_node(this->node, true);
      executor.cancel();
      spin_exited = true;
    });

  // Do some work for longer than the future needs.
  for (int i = 0; i < 100; ++i) {
    this->publisher->publish(test_msgs::msg::Empty());
    std::this_thread::sleep_for(1ms);
    if (spin_exited) {
      break;
    }
  }

  // Not testing accuracy, just want to make sure that some work occurred.
  EXPECT_LT(0, this->callback_count);

  // If this fails, the test will probably crash because spinner goes out of scope while the thread
  // is active. However, it beats letting this run until the gtest timeout.
  ASSERT_TRUE(spin_exited);
  executor.cancel();
  spinner.join();
}

// Check spin_until_future_complete timeout works as expected
TYPED_TEST(TestExecutors, testSpinUntilFutureCompleteWithTimeout)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  bool spin_exited = false;

  // Needs to run longer than spin_until_future_complete's timeout.
  std::future<void> future = std::async(
    std::launch::async,
    [&spin_exited]() {
      auto start = std::chrono::steady_clock::now();
      while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
        std::this_thread::sleep_for(1ms);
      }
    });

  // Short timeout
  std::thread spinner([&]() {
      auto ret = executor.spin_until_future_complete(future, 1ms);
      EXPECT_EQ(rclcpp::FutureReturnCode::TIMEOUT, ret);
      executor.remove_node(this->node, true);
      spin_exited = true;
    });

  // Do some work for longer than timeout needs.
  for (int i = 0; i < 100; ++i) {
    this->publisher->publish(test_msgs::msg::Empty());
    std::this_thread::sleep_for(1ms);
    if (spin_exited) {
      break;
    }
  }

  EXPECT_TRUE(spin_exited);
  spinner.join();
}

class TestWaitable : public rclcpp::Waitable
{
public:
  TestWaitable() = default;

  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override
  {
    rclcpp::detail::add_guard_condition_to_rcl_wait_set(*wait_set, gc_);
  }

  void trigger()
  {
    gc_.trigger();
  }

  bool
  is_ready(rcl_wait_set_t * wait_set) override
  {
    (void)wait_set;
    return true;
  }

  std::shared_ptr<void>
  take_data() override
  {
    return nullptr;
  }

  std::shared_ptr<void>
  take_data_by_entity_id(size_t id) override
  {
    (void) id;
    return nullptr;
  }

  void
  execute(std::shared_ptr<void> & data) override
  {
    (void) data;
    count_++;
    std::this_thread::sleep_for(3ms);
  }

  void
  set_on_ready_callback(std::function<void(size_t, int)> callback) override
  {
    auto gc_callback = [callback](size_t count) {
        callback(count, 0);
      };
    gc_.set_on_trigger_callback(gc_callback);
  }

  void
  clear_on_ready_callback() override
  {
    gc_.set_on_trigger_callback(nullptr);
  }

  size_t
  get_number_of_ready_guard_conditions() override {return 1;}

  size_t
  get_count()
  {
    return count_;
  }

private:
  size_t count_ = 0;
  rclcpp::GuardCondition gc_;
};

TYPED_TEST(TestExecutors, spinAll)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  auto waitable_interfaces = this->node->get_node_waitables_interface();
  auto my_waitable = std::make_shared<TestWaitable>();
  waitable_interfaces->add_waitable(my_waitable, nullptr);
  executor.add_node(this->node);

  // Long timeout, but should not block test if spin_all works as expected as we cancel the
  // executor.
  bool spin_exited = false;
  std::thread spinner([&spin_exited, &executor, this]() {
      executor.spin_all(1s);
      executor.remove_node(this->node, true);
      spin_exited = true;
    });

  // Do some work until sufficient calls to the waitable occur
  auto start = std::chrono::steady_clock::now();
  while (
    my_waitable->get_count() <= 1 &&
    !spin_exited &&
    (std::chrono::steady_clock::now() - start < 1s))
  {
    my_waitable->trigger();
    this->publisher->publish(test_msgs::msg::Empty());
    std::this_thread::sleep_for(1ms);
  }

  executor.cancel();
  start = std::chrono::steady_clock::now();
  while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_LT(1u, my_waitable->get_count());
  waitable_interfaces->remove_waitable(my_waitable, nullptr);
  ASSERT_TRUE(spin_exited);
  spinner.join();
}

TYPED_TEST(TestExecutors, spinSome)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  auto waitable_interfaces = this->node->get_node_waitables_interface();
  auto my_waitable = std::make_shared<TestWaitable>();
  waitable_interfaces->add_waitable(my_waitable, nullptr);
  executor.add_node(this->node);

  // Long timeout, doesn't block test from finishing because spin_some should exit after the
  // first one completes.
  bool spin_exited = false;
  std::thread spinner([&spin_exited, &executor, this]() {
      executor.spin_some(1s);
      executor.remove_node(this->node, true);
      spin_exited = true;
    });

  // Do some work until sufficient calls to the waitable occur, but keep going until either
  // count becomes too large, spin exits, or the 1 second timeout completes.
  auto start = std::chrono::steady_clock::now();
  while (
    my_waitable->get_count() <= 1 &&
    !spin_exited &&
    (std::chrono::steady_clock::now() - start < 1s))
  {
    my_waitable->trigger();
    this->publisher->publish(test_msgs::msg::Empty());
    std::this_thread::sleep_for(1ms);
  }
  // The count of "execute" depends on whether the executor starts spinning before (1) or after (0)
  // the first iteration of the while loop
  EXPECT_LE(1u, my_waitable->get_count());
  waitable_interfaces->remove_waitable(my_waitable, nullptr);
  EXPECT_TRUE(spin_exited);
  // Cancel if it hasn't exited already.
  executor.cancel();

  spinner.join();
}

// Check spin_node_until_future_complete with node base pointer
TYPED_TEST(TestExecutors, testSpinNodeUntilFutureCompleteNodeBasePtr)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;

  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);

  auto shared_future = future.share();
  auto ret = rclcpp::executors::spin_node_until_future_complete(
    executor, this->node->get_node_base_interface(), shared_future, 1s);
  EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
}

// Check spin_node_until_future_complete with node pointer
TYPED_TEST(TestExecutors, testSpinNodeUntilFutureCompleteNodePtr)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;

  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);

  auto shared_future = future.share();
  auto ret = rclcpp::executors::spin_node_until_future_complete(
    executor, this->node, shared_future, 1s);
  EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
}

// Check spin_until_future_complete can be properly interrupted.
TYPED_TEST(TestExecutors, testSpinUntilFutureCompleteInterrupted)
{
  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  bool spin_exited = false;

  // This needs to block longer than it takes to get to the shutdown call below and for
  // spin_until_future_complete to return
  std::future<void> future = std::async(
    std::launch::async,
    [&spin_exited]() {
      auto start = std::chrono::steady_clock::now();
      while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
        std::this_thread::sleep_for(1ms);
      }
    });

  // Long timeout
  std::thread spinner([&spin_exited, &executor, &future]() {
      auto ret = executor.spin_until_future_complete(future, 1s);
      EXPECT_EQ(rclcpp::FutureReturnCode::INTERRUPTED, ret);
      spin_exited = true;
    });

  // Do some minimal work
  this->publisher->publish(test_msgs::msg::Empty());
  std::this_thread::sleep_for(1ms);

  // Force interruption
  rclcpp::shutdown();

  // Give it time to exit
  auto start = std::chrono::steady_clock::now();
  while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(spin_exited);
  spinner.join();
}

// Check if services work as expected
TYPED_TEST(TestExecutors, testService)
{
  using ExecutorType = TypeParam;
  // rmw_connextdds doesn't support events-executor
  if (
    std::is_same<ExecutorType, rclcpp::experimental::executors::EventsExecutor>() &&
    std::string(rmw_get_implementation_identifier()).find("rmw_connextdds") == 0)
  {
    GTEST_SKIP();
  }

  ExecutorType executor;
  executor.add_node(this->node);

  bool spin_exited = false;

  rclcpp::Node::SharedPtr node = this->node;

  const std::string service_name("/test/test_service");

  using Service = test_msgs::srv::Empty;

  bool gotCallback = false;

  auto service_cb = [&gotCallback] (const std::shared_ptr<Service::Request> /*request*/,
         std::shared_ptr<Service::Response>      /*response*/)
  {
    gotCallback = true;
  };

  auto service = node->create_service<Service>(service_name, service_cb, rclcpp::ServicesQoS());

  // Long timeout
  std::thread spinner([&spin_exited, &executor]() {
      executor.spin();
      spin_exited = true;
    });

  std::this_thread::sleep_for(1ms);

  const std::shared_ptr<Service::Request> req = std::make_shared<Service::Request>();

  auto client = node->create_client<Service>(service_name);


  EXPECT_TRUE(client->wait_for_service(30ms));

  auto handle = client->async_send_request(req);

  auto retCode = handle.wait_for(500ms);
  EXPECT_EQ(retCode, std::future_status::ready);

  EXPECT_TRUE(gotCallback);

  // Force interruption
  rclcpp::shutdown();

  // Give it time to exit
  auto start = std::chrono::steady_clock::now();
  while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(spin_exited);
  spinner.join();
}

//test if objects work that were added after spinning started
TYPED_TEST(TestExecutors, addAfterSpin)
{
  using ExecutorType = TypeParam;
  // rmw_connextdds doesn't support events-executor
  if (
    std::is_same<ExecutorType, rclcpp::experimental::executors::EventsExecutor>() &&
    std::string(rmw_get_implementation_identifier()).find("rmw_connextdds") == 0)
  {
    GTEST_SKIP();
  }

  ExecutorType executor;
  executor.add_node(this->node);

  bool spin_exited = false;


  // Long timeout
  std::thread spinner([&spin_exited, &executor]() {
      executor.spin();
      spin_exited = true;
    });

  std::this_thread::sleep_for(10ms);

  rclcpp::Node::SharedPtr node = this->node;

  const std::string service_name("/test/test_service");

  using Service = test_msgs::srv::Empty;

  bool gotCallback = false;

  auto service_cb = [&gotCallback] (const std::shared_ptr<Service::Request> /*request*/,
         std::shared_ptr<Service::Response>      /*response*/)
  {
    gotCallback = true;
  };

  auto service = node->create_service<Service>(service_name, service_cb, rclcpp::ServicesQoS());

  const std::shared_ptr<Service::Request> req = std::make_shared<Service::Request>();

  auto client = node->create_client<Service>(service_name);


  EXPECT_TRUE(client->wait_for_service(30ms));

  auto handle = client->async_send_request(req);

  auto retCode = handle.wait_for(500ms);
  EXPECT_EQ(retCode, std::future_status::ready);

  EXPECT_TRUE(gotCallback);

  // Force interruption
  rclcpp::shutdown();

  // Give it time to exit
  auto start = std::chrono::steady_clock::now();
  while (!spin_exited && (std::chrono::steady_clock::now() - start) < 1s) {
    std::this_thread::sleep_for(1ms);
  }

  EXPECT_TRUE(spin_exited);
  spinner.join();
}

// This test verifies that the add_node operation is robust wrt race conditions.
// It's mostly meant to prevent regressions in the events-executor, but the operation should be
// thread-safe in all executor implementations.
// The initial implementation of the events-executor contained a bug where the executor
// would end up in an inconsistent state and stop processing interrupt/shutdown notifications.
// Manually adding a node to the executor results in a) producing a notify waitable event
// and b) refreshing the executor collections.
// The inconsistent state would happen if the event was processed before the collections were
// finished to be refreshed: the executor would pick up the event but be unable to process it.
// This would leave the `notify_waitable_event_pushed_` flag to true, preventing additional
// notify waitable events to be pushed.
// The behavior is observable only under heavy load, so this test spawns several worker
// threads. Due to the nature of the bug, this test may still succeed even if the
// bug is present. However repeated runs will show its flakiness nature and indicate
// an eventual regression.
TYPED_TEST(TestExecutors, testRaceConditionAddNode)
{
  using ExecutorType = TypeParam;
  // rmw_connextdds doesn't support events-executor
  if (
    std::is_same<ExecutorType, rclcpp::experimental::executors::EventsExecutor>() &&
    std::string(rmw_get_implementation_identifier()).find("rmw_connextdds") == 0)
  {
    GTEST_SKIP();
  }

  // Spawn some threads to do some heavy work
  std::atomic<bool> should_cancel = false;
  std::vector<std::thread> stress_threads;
  for (size_t i = 0; i < 5 * std::thread::hardware_concurrency(); i++) {
    stress_threads.emplace_back(
      [&should_cancel, i]() {
        // This is just some arbitrary heavy work
        volatile size_t total = 0;
        for (size_t k = 0; k < 549528914167; k++) {
          if (should_cancel) {
            break;
          }
          total += k * (i + 42);
          (void)total;
        }
      });
  }

  // Create an executor
  auto executor = std::make_shared<ExecutorType>();
  // Start spinning
  auto executor_thread = std::thread(
    [executor]() {
      executor->spin();
    });
  // Add a node to the executor
  executor->add_node(this->node);

  // Cancel the executor (make sure that it's already spinning first)
  while (!executor->is_spinning() && rclcpp::ok()) {
    continue;
  }
  executor->cancel();

  // Try to join the thread after cancelling the executor
  // This is the "test". We want to make sure that we can still cancel the executor
  // regardless of the presence of race conditions
  executor_thread.join();

  // The test is now completed: we can join the stress threads
  should_cancel = true;
  for (auto & t : stress_threads) {
    t.join();
  }
}

// Check spin_until_future_complete with node base pointer (instantiates its own executor)
TEST(TestExecutors, testSpinUntilFutureCompleteNodeBasePtr)
{
  rclcpp::init(0, nullptr);

  {
    auto node = std::make_shared<rclcpp::Node>("node");

    std::promise<bool> promise;
    std::future<bool> future = promise.get_future();
    promise.set_value(true);

    auto shared_future = future.share();
    auto ret = rclcpp::spin_until_future_complete(
      node->get_node_base_interface(), shared_future, 1s);
    EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
  }

  rclcpp::shutdown();
}

// Check spin_until_future_complete with node pointer (instantiates its own executor)
TEST(TestExecutors, testSpinUntilFutureCompleteNodePtr)
{
  rclcpp::init(0, nullptr);

  {
    auto node = std::make_shared<rclcpp::Node>("node");

    std::promise<bool> promise;
    std::future<bool> future = promise.get_future();
    promise.set_value(true);

    auto shared_future = future.share();
    auto ret = rclcpp::spin_until_future_complete(node, shared_future, 1s);
    EXPECT_EQ(rclcpp::FutureReturnCode::SUCCESS, ret);
  }

  rclcpp::shutdown();
}

template<typename T>
class TestIntraprocessExecutors : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestCase()
  {
    rclcpp::shutdown();
  }

  void SetUp()
  {
    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::stringstream test_name;
    test_name << test_info->test_case_name() << "_" << test_info->name();
    node = std::make_shared<rclcpp::Node>("node", test_name.str());

    callback_count = 0u;

    const std::string topic_name = std::string("topic_") + test_name.str();

    rclcpp::PublisherOptions po;
    po.use_intra_process_comm = rclcpp::IntraProcessSetting::Enable;
    publisher = node->create_publisher<test_msgs::msg::Empty>(topic_name, rclcpp::QoS(1), po);

    auto callback = [this](test_msgs::msg::Empty::ConstSharedPtr) {
        this->callback_count.fetch_add(1u);
      };

    rclcpp::SubscriptionOptions so;
    so.use_intra_process_comm = rclcpp::IntraProcessSetting::Enable;
    subscription =
      node->create_subscription<test_msgs::msg::Empty>(
      topic_name, rclcpp::QoS(kNumMessages), std::move(callback), so);
  }

  void TearDown()
  {
    publisher.reset();
    subscription.reset();
    node.reset();
  }

  const size_t kNumMessages = 100;

  rclcpp::Node::SharedPtr node;
  rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr publisher;
  rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr subscription;
  std::atomic_size_t callback_count;
};

TYPED_TEST_SUITE(TestIntraprocessExecutors, ExecutorTypes, ExecutorTypeNames);

TYPED_TEST(TestIntraprocessExecutors, testIntraprocessRetrigger) {
  // This tests that executors will continue to service intraprocess subscriptions in the case
  // that publishers aren't continuing to publish.
  // This was previously broken in that intraprocess guard conditions were only triggered on
  // publish and the test was added to prevent future regressions.
  static constexpr size_t kNumMessages = 100;

  using ExecutorType = TypeParam;
  ExecutorType executor;
  executor.add_node(this->node);

  EXPECT_EQ(0u, this->callback_count.load());
  this->publisher->publish(test_msgs::msg::Empty());

  // Wait for up to 5 seconds for the first message to come available.
  const std::chrono::milliseconds sleep_per_loop(10);
  int loops = 0;
  while (1u != this->callback_count.load() && loops < 500) {
    rclcpp::sleep_for(sleep_per_loop);
    executor.spin_some();
    loops++;
  }
  EXPECT_EQ(1u, this->callback_count.load());

  // reset counter
  this->callback_count.store(0u);

  for (size_t ii = 0; ii < kNumMessages; ++ii) {
    this->publisher->publish(test_msgs::msg::Empty());
  }

  // Fire a timer every 10ms up to 5 seconds waiting for subscriptions to be read.
  loops = 0;
  auto timer = this->node->create_wall_timer(
    std::chrono::milliseconds(10), [this, &executor, &loops]() {
      loops++;
      if (kNumMessages == this->callback_count.load() || loops == 500) {
        executor.cancel();
      }
    });
  executor.spin();
  EXPECT_EQ(kNumMessages, this->callback_count.load());
}

class TimerNode : public rclcpp::Node
{
public:
  explicit TimerNode(std::string subname)
  : Node("timer_node", subname)
  {
    timer1_ = rclcpp::create_timer(
      this->get_node_base_interface(), get_node_timers_interface(),
      get_clock(), 1ms,
      std::bind(&TimerNode::Timer1Callback, this));

    timer2_ =
      rclcpp::create_timer(
      this->get_node_base_interface(), get_node_timers_interface(),
      get_clock(), 1ms,
      std::bind(&TimerNode::Timer2Callback, this));
  }

  int GetTimer1Cnt() {return cnt1_;}
  int GetTimer2Cnt() {return cnt2_;}

  void ResetTimer1()
  {
    timer1_->reset();
  }

  void ResetTimer2()
  {
    timer2_->reset();
  }

  void CancelTimer1()
  {
    RCLCPP_DEBUG(this->get_logger(), "Timer 1 cancelling!");
    timer1_->cancel();
  }

  void CancelTimer2()
  {
    RCLCPP_DEBUG(this->get_logger(), "Timer 2 cancelling!");
    timer2_->cancel();
  }

private:
  void Timer1Callback()
  {
    RCLCPP_DEBUG(this->get_logger(), "Timer 1!");
    cnt1_++;
  }

  void Timer2Callback()
  {
    RCLCPP_DEBUG(this->get_logger(), "Timer 2!");
    cnt2_++;
  }

  rclcpp::TimerBase::SharedPtr timer1_;
  rclcpp::TimerBase::SharedPtr timer2_;
  int cnt1_ = 0;
  int cnt2_ = 0;
};

// Sets up a separate thread to publish /clock messages.
// Clock rate relative to real clock is controlled by realtime_update_rate.
// This is set conservatively slow to ensure unit tests are reliable on Windows
// environments, where timing performance is subpar.
//
// Use `sleep_for` in tests to advance the clock. Clock should run and be published
// in separate thread continuously to ensure correct behavior in node under test.
class ClockPublisher : public rclcpp::Node
{
public:
  explicit ClockPublisher(float simulated_clock_step = .001f, float realtime_update_rate = 0.25f)
  : Node("clock_publisher"),
    ros_update_duration_(0, 0),
    realtime_clock_step_(0, 0),
    rostime_(0, 0)
  {
    clock_publisher_ = this->create_publisher<rosgraph_msgs::msg::Clock>("clock", 10);
    realtime_clock_step_ =
      rclcpp::Duration::from_seconds(simulated_clock_step / realtime_update_rate);
    ros_update_duration_ = rclcpp::Duration::from_seconds(simulated_clock_step);

    timer_thread_ = std::thread(&ClockPublisher::RunTimer, this);
  }

  ~ClockPublisher()
  {
    running_ = false;
    if (timer_thread_.joinable()) {
      timer_thread_.join();
    }
  }

  void sleep_for(rclcpp::Duration duration)
  {
    rclcpp::Time start_time(0, 0, RCL_ROS_TIME);
    {
      const std::lock_guard<std::mutex> lock(mutex_);
      start_time = rostime_;
    }
    rclcpp::Time current_time = start_time;

    while (true) {
      {
        const std::lock_guard<std::mutex> lock(mutex_);
        current_time = rostime_;
      }
      if ((current_time - start_time) >= duration) {
        return;
      }
      std::this_thread::sleep_for(realtime_clock_step_.to_chrono<std::chrono::milliseconds>());
      rostime_ += ros_update_duration_;
    }
  }

private:
  void RunTimer()
  {
    while (running_) {
      PublishClock();
      std::this_thread::sleep_for(realtime_clock_step_.to_chrono<std::chrono::milliseconds>());
    }
  }

  void PublishClock()
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    auto message = rosgraph_msgs::msg::Clock();
    message.clock = rostime_;
    clock_publisher_->publish(message);
  }

  rclcpp::Publisher<rosgraph_msgs::msg::Clock>::SharedPtr clock_publisher_;

  rclcpp::Duration ros_update_duration_;
  rclcpp::Duration realtime_clock_step_;
  // Rostime must be guarded by a mutex, since accessible in running thread
  // as well as sleep_for
  rclcpp::Time rostime_;
  std::mutex mutex_;
  std::thread timer_thread_;
  std::atomic<bool> running_ = true;
};


template<typename T>
class TestTimerCancelBehavior : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestCase()
  {
    rclcpp::shutdown();
  }

  void SetUp()
  {
    const auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::stringstream test_name;
    test_name << test_info->test_case_name() << "_" << test_info->name();
    node = std::make_shared<TimerNode>(test_name.str());
    param_client = std::make_shared<rclcpp::SyncParametersClient>(node);
    ASSERT_TRUE(param_client->wait_for_service(5s));

    auto set_parameters_results = param_client->set_parameters(
      {rclcpp::Parameter("use_sim_time", false)});
    for (auto & result : set_parameters_results) {
      ASSERT_TRUE(result.successful);
    }

    // Run standalone thread to publish clock time
    sim_clock_node = std::make_shared<ClockPublisher>();

    // Spin the executor in a standalone thread
    executor.add_node(this->node);
    standalone_thread = std::thread(
      [this]() {
        executor.spin();
      });
  }

  void TearDown()
  {
    node.reset();

    // Clean up thread object
    if (standalone_thread.joinable()) {
      standalone_thread.join();
    }
  }

  std::shared_ptr<TimerNode> node;
  std::shared_ptr<ClockPublisher> sim_clock_node;
  rclcpp::SyncParametersClient::SharedPtr param_client;
  std::thread standalone_thread;
  T executor;
};

TYPED_TEST_SUITE(TestTimerCancelBehavior, ExecutorTypes, ExecutorTypeNames);

TYPED_TEST(TestTimerCancelBehavior, testTimer1CancelledWithExecutorSpin) {
  // Validate that cancelling one timer yields no change in behavior for other
  // timers. Specifically, this tests the behavior when using spin() to run the
  // executor, which is the most common usecase.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer1();
  this->sim_clock_node->sleep_for(150ms);
  this->executor.cancel();

  int t1_runs = this->node->GetTimer1Cnt();
  int t2_runs = this->node->GetTimer2Cnt();
  EXPECT_NE(t1_runs, t2_runs);
  // Check that t2 has significantly more calls
  EXPECT_LT(t1_runs + 50, t2_runs);
}

TYPED_TEST(TestTimerCancelBehavior, testTimer2CancelledWithExecutorSpin) {
  // Validate that cancelling one timer yields no change in behavior for other
  // timers. Specifically, this tests the behavior when using spin() to run the
  // executor, which is the most common usecase.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer2();
  this->sim_clock_node->sleep_for(150ms);
  this->executor.cancel();

  int t1_runs = this->node->GetTimer1Cnt();
  int t2_runs = this->node->GetTimer2Cnt();
  EXPECT_NE(t1_runs, t2_runs);
  // Check that t1 has significantly more calls
  EXPECT_LT(t2_runs + 50, t1_runs);
}

TYPED_TEST(TestTimerCancelBehavior, testHeadTimerCancelThenResetBehavior) {
  // Validate that cancelling timer doesn't affect operation of other timers,
  // and that the cancelled timer starts executing normally once reset manually.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer1();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_initial = this->node->GetTimer1Cnt();
  int t2_runs_initial = this->node->GetTimer2Cnt();

  // Manually reset timer 1, then sleep again
  // Counts should update.
  this->node->ResetTimer1();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_final = this->node->GetTimer1Cnt();
  int t2_runs_final = this->node->GetTimer2Cnt();

  this->executor.cancel();

  // T1 should have been restarted, and execute about 15 additional times.
  // Check 10 greater than initial, to account for some timing jitter.
  EXPECT_LT(t1_runs_initial + 50, t1_runs_final);

  EXPECT_LT(t1_runs_initial + 50, t2_runs_initial);
  // Check that t2 has significantly more calls, and keeps getting called.
  EXPECT_LT(t2_runs_initial + 50, t2_runs_final);
}

TYPED_TEST(TestTimerCancelBehavior, testBackTimerCancelThenResetBehavior) {
  // Validate that cancelling timer doesn't affect operation of other timers,
  // and that the cancelled timer starts executing normally once reset manually.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_initial = this->node->GetTimer1Cnt();
  int t2_runs_initial = this->node->GetTimer2Cnt();

  // Manually reset timer 1, then sleep again
  // Counts should update.
  this->node->ResetTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_final = this->node->GetTimer1Cnt();
  int t2_runs_final = this->node->GetTimer2Cnt();

  this->executor.cancel();

  // T2 should have been restarted, and execute about 15 additional times.
  // Check 10 greater than initial, to account for some timing jitter.
  EXPECT_LT(t2_runs_initial + 50, t2_runs_final);

  EXPECT_LT(t2_runs_initial + 50, t1_runs_initial);
  // Check that t1 has significantly more calls, and keeps getting called.
  EXPECT_LT(t1_runs_initial + 50, t1_runs_final);
}

TYPED_TEST(TestTimerCancelBehavior, testBothTimerCancelThenResetT1Behavior) {
  // Validate behavior from cancelling 2 timers, then only re-enabling one of them.
  // Ensure that only the reset timer is executed.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer1();
  this->node->CancelTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_initial = this->node->GetTimer1Cnt();
  int t2_runs_initial = this->node->GetTimer2Cnt();

  // Manually reset timer 1, then sleep again
  // Counts should update.
  this->node->ResetTimer1();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_intermediate = this->node->GetTimer1Cnt();
  int t2_runs_intermediate = this->node->GetTimer2Cnt();

  this->node->ResetTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_final = this->node->GetTimer1Cnt();
  int t2_runs_final = this->node->GetTimer2Cnt();

  this->executor.cancel();

  // T1 and T2 should have the same initial count.
  EXPECT_LE(std::abs(t1_runs_initial - t2_runs_initial), 1);

  // Expect that T1 has up to 15 more calls than t2. Add some buffer
  // to account for jitter.
  EXPECT_EQ(t2_runs_initial, t2_runs_intermediate);
  EXPECT_LT(t1_runs_initial + 50, t1_runs_intermediate);

  // Expect that by end of test, both are running properly again.
  EXPECT_LT(t1_runs_intermediate + 50, t1_runs_final);
  EXPECT_LT(t2_runs_intermediate + 50, t2_runs_final);
}

TYPED_TEST(TestTimerCancelBehavior, testBothTimerCancelThenResetT2Behavior) {
  // Validate behavior from cancelling 2 timers, then only re-enabling one of them.
  // Ensure that only the reset timer is executed.

  // Cancel to stop the spin after some time.
  this->sim_clock_node->sleep_for(50ms);
  this->node->CancelTimer1();
  this->node->CancelTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_initial = this->node->GetTimer1Cnt();
  int t2_runs_initial = this->node->GetTimer2Cnt();

  // Manually reset timer 1, then sleep again
  // Counts should update.
  this->node->ResetTimer2();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_intermediate = this->node->GetTimer1Cnt();
  int t2_runs_intermediate = this->node->GetTimer2Cnt();

  this->node->ResetTimer1();
  this->sim_clock_node->sleep_for(150ms);
  int t1_runs_final = this->node->GetTimer1Cnt();
  int t2_runs_final = this->node->GetTimer2Cnt();

  this->executor.cancel();

  // T1 and T2 should have the same initial count.
  EXPECT_LE(std::abs(t1_runs_initial - t2_runs_initial), 1);

  // Expect that T1 has up to 15 more calls than t2. Add some buffer
  // to account for jitter.
  EXPECT_EQ(t1_runs_initial, t1_runs_intermediate);
  EXPECT_LT(t2_runs_initial + 50, t2_runs_intermediate);

  // Expect that by end of test, both are running properly again.
  EXPECT_LT(t1_runs_intermediate + 50, t1_runs_final);
  EXPECT_LT(t2_runs_intermediate + 50, t2_runs_final);
}
