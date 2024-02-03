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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "performance_test_fixture/performance_test_fixture.hpp"

#include "rclcpp/rclcpp.hpp"
#include "rclcpp/executors/cbg_executor.hpp"
#include "rcpputils/scope_exit.hpp"
#include "test_msgs/msg/empty.hpp"

using namespace std::chrono_literals;
using performance_test_fixture::PerformanceTest;

constexpr unsigned int kNumberOfNodes = 10;
constexpr unsigned int kNumberOfPubSubs = 10;

class PerformanceTestExecutor : public PerformanceTest
{
public:
  void SetUp(benchmark::State & st)
  {
    rclcpp::init(0, nullptr);
    callback_count = 0;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      nodes.push_back(std::make_shared<rclcpp::Node>("my_node_" + std::to_string(i)));

      for (unsigned int j = 0u; j < kNumberOfPubSubs; j++) {
        publishers.push_back(
          nodes[i]->create_publisher<test_msgs::msg::Empty>(
            "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10)));

        auto callback = [this](test_msgs::msg::Empty::ConstSharedPtr) {this->callback_count++;};
        subscriptions.push_back(
          nodes[i]->create_subscription<test_msgs::msg::Empty>(
            "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10), std::move(callback)));
      }
    }
    PerformanceTest::SetUp(st);
  }
  void TearDown(benchmark::State & st)
  {
    PerformanceTest::TearDown(st);
    subscriptions.clear();
    publishers.clear();
    nodes.clear();
    rclcpp::shutdown();
  }

  template<class Executer>
  void executor_spin_some(benchmark::State & st)
  {
    Executer executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
    }
    while(subscriptions.front()->get_publisher_count() == 0)
    {
      std::this_thread::sleep_for(10ms);
    }

    // precompute executer internals
    publishers[0]->publish(empty_msgs);
    executor.spin_some(100ms);

    if (callback_count == 0) {
      st.SkipWithError("No message was received");
    }

    callback_count = 0;
    reset_heap_counters();

    for (auto _ : st) {
      (void)_;
      st.PauseTiming();
      for (auto &pub : publishers) {
        pub->publish(empty_msgs);
      }
//       std::this_thread::sleep_for(10ms);
      std::this_thread::yield();
      st.ResumeTiming();

      callback_count = 0;
      executor.spin_some(1000ms);
      if(callback_count < publishers.size())
      {
        st.SkipWithError("Not all messages were received");
      }
    }
    if (callback_count == 0) {
      st.SkipWithError("No message was received");
    }
  }

  template<class Executer>
  void benchmark_wait_for_work(benchmark::State & st)
  {
    class ExecuterDerived : public Executer
    {
public:
      void call_wait_for_work(std::chrono::nanoseconds timeout)
      {
        Executer::wait_for_work(timeout);
      }
    };

    ExecuterDerived executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
    }
    while(subscriptions.front()->get_publisher_count() == 0)
    {
      std::this_thread::sleep_for(10ms);
    }
    // we need one ready event
    publishers[0]->publish(empty_msgs);
    executor.spin_some(100ms);
    if (callback_count == 0) {
      st.SkipWithError("No message was received");
    }

    reset_heap_counters();

    for (auto _ : st) {
      (void)_;

      st.PauseTiming();
      // we need one ready event
      publishers[0]->publish(empty_msgs);
      st.ResumeTiming();


      executor.call_wait_for_work(100ms);
      st.PauseTiming();
      executor.spin_some(100ms);
      st.ResumeTiming();
    }
  }

  template<class Executer>
  void benchmark_wait_for_work_force_rebuild(benchmark::State & st)
  {
    class ExecuterDerived : public Executer
    {
public:
      void call_wait_for_work(std::chrono::nanoseconds timeout)
      {
        Executer::wait_for_work(timeout);
      }
    };

    ExecuterDerived executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
    }

    while(subscriptions.front()->get_publisher_count() == 0)
    {
      std::this_thread::sleep_for(10ms);
    }

    // we need one ready event
    publishers[0]->publish(empty_msgs);

    executor.spin_some(100ms);
    if (callback_count == 0) {
      st.SkipWithError("No message was received");
    }

    // only need to force a waitset rebuild
    rclcpp::PublisherBase::SharedPtr somePub;

    reset_heap_counters();

//     std::cout << "Perf loop start !!!!!!!!!" << std::endl;

    for (auto _ : st) {
      (void)_;

      st.PauseTiming();
      somePub = nodes[0]->create_publisher<test_msgs::msg::Empty>(
            "/foo", rclcpp::QoS(10));
      // we need one ready event
      publishers[0]->publish(empty_msgs);
      st.ResumeTiming();


      executor.call_wait_for_work(std::chrono::nanoseconds::zero());
      st.PauseTiming();
      executor.spin_some(100ms);
      st.ResumeTiming();
    }
  }
  test_msgs::msg::Empty empty_msgs;
  std::vector<rclcpp::Node::SharedPtr> nodes;
  std::vector<rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr> publishers;
  std::vector<rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr> subscriptions;
  uint callback_count;
};


BENCHMARK_F(PerformanceTestExecutor, single_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutor, static_single_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutor, multi_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutor, cbg_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::CBGExecutor>(st);
}


BENCHMARK_F(PerformanceTestExecutor, single_thread_executor_wait_for_work)(benchmark::State & st)
{
  benchmark_wait_for_work<rclcpp::executors::SingleThreadedExecutor>(st);
}

// BENCHMARK_F(PerformanceTestExecutor, static_single_thread_executor_wait_for_work)(benchmark::State & st)
// {
//   benchmark_wait_for_work<rclcpp::executors::StaticSingleThreadedExecutor>(st);
// }

BENCHMARK_F(PerformanceTestExecutor, multi_thread_executor_wait_for_work)(benchmark::State & st)
{
  benchmark_wait_for_work<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutor, cbg_executor_wait_for_work)(benchmark::State & st)
{
  benchmark_wait_for_work<rclcpp::executors::CBGExecutor>(st);
}


BENCHMARK_F(PerformanceTestExecutor, single_thread_executor_wait_for_work_rebuild)(benchmark::State & st)
{
  benchmark_wait_for_work_force_rebuild<rclcpp::executors::SingleThreadedExecutor>(st);
}

// BENCHMARK_F(PerformanceTestExecutor, static_single_thread_executor_wait_for_work_rebuild)(benchmark::State & st)
// {
//   benchmark_wait_for_work_force_rebuild<rclcpp::executors::StaticSingleThreadedExecutor>(st);
// }

BENCHMARK_F(PerformanceTestExecutor, multi_thread_executor_wait_for_work_rebuild)(benchmark::State & st)
{
  benchmark_wait_for_work_force_rebuild<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutor, cbg_executor_wait_for_work_rebuild)(benchmark::State & st)
{
  benchmark_wait_for_work_force_rebuild<rclcpp::executors::CBGExecutor>(st);
}

class CallbackWaitable : public rclcpp::Waitable
{
public:
  CallbackWaitable() = default;

  void
  add_to_wait_set(rcl_wait_set_t * wait_set) override
  {

    rcl_ret_t ret = rcl_wait_set_add_guard_condition(wait_set, &gc.get_rcl_guard_condition(), &gc_waitset_idx);
    if (RCL_RET_OK != ret) {
      rclcpp::exceptions::throw_from_rcl_error(
        ret, "failed to add guard condition to wait set");
    }

    if(has_trigger)
    {
      gc.trigger();
    }

  }

  void trigger()
  {
//     std::cout << "Trigger called for idx " << gc_waitset_idx << std::endl;
    has_trigger = true;
    gc.trigger();
  }

  bool
  is_ready(rcl_wait_set_t * wait_set) override
  {
//     std::cout << "Is ready called for idx " << gc_waitset_idx << std::endl;
    return wait_set->guard_conditions[gc_waitset_idx];
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
//     std::cout << "execute called for idx " << gc_waitset_idx << std::endl;
    has_trigger = false;
    (void) data;
    if(cb_fun)
    {
      cb_fun();
    }
  }

  void set_execute_callback_function(std::function<void (void)> cb_fun_in)
  {
    cb_fun = std::move(cb_fun_in);
  }

  size_t
  get_number_of_ready_guard_conditions() override {return 1;}


private:
  std::atomic<bool> has_trigger = false;
  std::function<void (void)> cb_fun;
  size_t gc_waitset_idx = 0;
  rclcpp::GuardCondition gc;

};

class CascadedPerformanceTestExecutor : public PerformanceTest
{
  std::condition_variable cascase_done;
  std::vector<std::shared_ptr<CallbackWaitable>> waitables;
  std::atomic<bool> last_cb_triggered;
public:
  void SetUp(benchmark::State & st)
  {
    rclcpp::init(0, nullptr);
    callback_count = 0;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      nodes.push_back(std::make_shared<rclcpp::Node>("my_node_" + std::to_string(i)));

      auto waitable_interfaces = nodes.back()->get_node_waitables_interface();

      auto callback_waitable = std::make_shared<CallbackWaitable>();
      waitables.push_back(callback_waitable);
      waitable_interfaces->add_waitable(callback_waitable, nullptr);

      publishers.push_back(
        nodes[i]->create_publisher<test_msgs::msg::Empty>(
          "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i), rclcpp::QoS(10)));

      auto callback = [this, i](test_msgs::msg::Empty::ConstSharedPtr) {
          if (i == kNumberOfNodes - 1) {
            this->callback_count++;
            cascase_done.notify_all();
          } else {
            publishers[i + 1]->publish(empty_msgs);
          }
        };
      subscriptions.push_back(
        nodes[i]->create_subscription<test_msgs::msg::Empty>(
          "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i), rclcpp::QoS(10), std::move(callback)));
    }
    for (unsigned int i = 0u; i < waitables.size(); i++) {
          if (i == waitables.size() - 1) {
            waitables[i]->set_execute_callback_function([this]() {
              last_cb_triggered = true;
              cascase_done.notify_all();
//               std::cout << "Notify done " << std::endl;
            });

          } else {
            waitables[i]->set_execute_callback_function([this, i]() {
//               std::cout << "Triggering callback " << i+1 << std::endl;
              waitables[i+1]->trigger();

            });
          }

    }
    PerformanceTest::SetUp(st);
  }
  void TearDown(benchmark::State & st)
  {
    PerformanceTest::TearDown(st);
    subscriptions.clear();
    publishers.clear();
    nodes.clear();
    waitables.clear();
    last_cb_triggered = false;
    rclcpp::shutdown();
  }

  template<class Executer>
  void executor_spin_some(benchmark::State & st)
  {
    Executer executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
    }

    // precompute the executor internals
    executor.spin_some(100ms);

    while(subscriptions.front()->get_publisher_count() == 0)
    {
      std::this_thread::sleep_for(10ms);
    }

    callback_count = 0;
    reset_heap_counters();

    std::mutex cond_mutex;
    std::unique_lock<std::mutex> lk(cond_mutex);
    auto thread = std::thread([&executor] {
//       std::cout << "Spin started" << std::endl;
      executor.spin();

//       std::cout << "Spin terminated" << std::endl;

    });

    for (auto _ : st) {
      (void)_;
      last_cb_triggered = false;

      // start the cascasde
      waitables[0]->trigger();

      cascase_done.wait_for(lk, 500ms);
//       std::cout << "waking up last cb was triggered : " << last_cb_triggered << std::endl;

      if (!last_cb_triggered) {
        st.SkipWithError("No message was received");
      }

    }

//     std::cout << "Stopping executor " << std::endl;

    executor.cancel();

    thread.join();

    if (!last_cb_triggered) {
      st.SkipWithError("No message was received");
    }
  }

  test_msgs::msg::Empty empty_msgs;
  std::vector<rclcpp::Node::SharedPtr> nodes;
  std::vector<rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr> publishers;
  std::vector<rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr> subscriptions;
  uint callback_count;
};


BENCHMARK_F(
  CascadedPerformanceTestExecutor,
  single_thread_executor_spin)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(
  CascadedPerformanceTestExecutor,
  static_single_thread_executor_spin)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

BENCHMARK_F(CascadedPerformanceTestExecutor, multi_thread_executor_spin)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(CascadedPerformanceTestExecutor, cbg_executor_spin)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::CBGExecutor>(st);
}


class PerformanceTestExecutorMultipleCallbackGroups : public PerformanceTest
{
public:
//   static constexpr unsigned int kNumberOfNodes = 20;

  void SetUp(benchmark::State & st)
  {
    rclcpp::init(0, nullptr);
    callback_count = 0;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      nodes.push_back(std::make_shared<rclcpp::Node>("my_node_" + std::to_string(i)));


      for (unsigned int j = 0u; j < kNumberOfPubSubs; j++) {
        callback_groups.push_back(
        nodes[i]->create_callback_group(
            rclcpp::CallbackGroupType::
            MutuallyExclusive));

        rclcpp::PublisherOptions pubOps;
        pubOps.callback_group = callback_groups.back();

        publishers.push_back(
          nodes[i]->create_publisher<test_msgs::msg::Empty>(
            "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10), pubOps));

        rclcpp::SubscriptionOptions subOps;
        subOps.callback_group = callback_groups.back();

        auto callback = [this](test_msgs::msg::Empty::ConstSharedPtr) {this->callback_count++;};
        subscriptions.push_back(
          nodes[i]->create_subscription<test_msgs::msg::Empty>(
            "/thread" + std::to_string(st.thread_index()) + "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10), std::move(callback), subOps));
      }
    }
    PerformanceTest::SetUp(st);
  }
  void TearDown(benchmark::State & st)
  {
    PerformanceTest::TearDown(st);
    subscriptions.clear();
    publishers.clear();
    callback_groups.clear();
    nodes.clear();
    rclcpp::shutdown();
  }

  template<class Executer>
  void executor_spin_some(benchmark::State & st)
  {
    Executer executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
    }

    publishers.front()->publish(empty_msgs);
    executor.spin_some(100ms);

    callback_count = 0;
    reset_heap_counters();

    for (auto _ : st) {
      (void)_;
      st.PauseTiming();
      for (auto &pub : publishers) {
        pub->publish(empty_msgs);
      }
      // wait for the messages to arrive
//       std::this_thread::sleep_for(10ms);
      std::this_thread::yield();
      st.ResumeTiming();

      callback_count = 0;
      executor.spin_some(500ms);
      if (callback_count != publishers.size()) {
        st.SkipWithError("Not all message were received");
      }
    }
    if (callback_count == 0) {
      st.SkipWithError("No message was received");
    }
  }

  test_msgs::msg::Empty empty_msgs;
  std::vector<rclcpp::Node::SharedPtr> nodes;
  std::vector<rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr> publishers;
  std::vector<rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr> subscriptions;
  std::vector<rclcpp::CallbackGroup::SharedPtr> callback_groups;
  uint callback_count;
};


BENCHMARK_F(
  PerformanceTestExecutorMultipleCallbackGroups,
  single_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorMultipleCallbackGroups,
  static_single_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorMultipleCallbackGroups,
  multi_thread_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorMultipleCallbackGroups,
  cbg_executor_spin_some)(benchmark::State & st)
{
  executor_spin_some<rclcpp::executors::CBGExecutor>(st);
}

class PerformanceTestExecutorSimple : public PerformanceTest
{
public:
  void SetUp(benchmark::State & st)
  {
    rclcpp::init(0, nullptr);
    node = std::make_shared<rclcpp::Node>("my_node");

    PerformanceTest::SetUp(st);
  }
  void TearDown(benchmark::State & st)
  {
    PerformanceTest::TearDown(st);
    node.reset();
    rclcpp::shutdown();
  }

  template<class Executor>
  void add_node(benchmark::State & st)
  {
    Executor executor;
    for (auto _ : st) {
      (void)_;
      executor.add_node(node);
      st.PauseTiming();
      executor.remove_node(node);
      st.ResumeTiming();
    }
  }

  template<class Executor>
  void remove_node(benchmark::State & st)
  {
    Executor executor;
    for (auto _ : st) {
      (void)_;
      st.PauseTiming();
      executor.add_node(node);
      st.ResumeTiming();
      executor.remove_node(node);
    }
  }


  template<class Executor>
  void spin_until_future_complete(benchmark::State & st)
  {
    Executor executor;
    // test success of an immediately finishing future
    std::promise<bool> promise;
    std::future<bool> future = promise.get_future();
    promise.set_value(true);
    auto shared_future = future.share();

    auto ret = executor.spin_until_future_complete(shared_future, 100ms);
    if (ret != rclcpp::FutureReturnCode::SUCCESS) {
      st.SkipWithError(rcutils_get_error_string().str);
    }

    reset_heap_counters();

    for (auto _ : st) {
      (void)_;
      ret = executor.spin_until_future_complete(shared_future, 100ms);
      if (ret != rclcpp::FutureReturnCode::SUCCESS) {
        st.SkipWithError(rcutils_get_error_string().str);
        break;
      }
    }

  }

  template<class Executor>
  void spin_node_until_future_complete(benchmark::State & st)
  {
    Executor executor;
    // test success of an immediately finishing future
    std::promise<bool> promise;
    std::future<bool> future = promise.get_future();
    promise.set_value(true);
    auto shared_future = future.share();

    auto ret = rclcpp::executors::spin_node_until_future_complete(
      executor, node, shared_future, 1s);
    if (ret != rclcpp::FutureReturnCode::SUCCESS) {
      st.SkipWithError(rcutils_get_error_string().str);
    }

    reset_heap_counters();

    for (auto _ : st) {
      (void)_;
      ret = rclcpp::executors::spin_node_until_future_complete(
        executor, node, shared_future, 1s);
      if (ret != rclcpp::FutureReturnCode::SUCCESS) {
        st.SkipWithError(rcutils_get_error_string().str);
        break;
      }
    }

  }

  rclcpp::Node::SharedPtr node;
};


BENCHMARK_F(PerformanceTestExecutorSimple, single_thread_executor_add_node)(benchmark::State & st)
{
  add_node<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple, single_thread_executor_remove_node)(benchmark::State & st)
{
  remove_node<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutorSimple, multi_thread_executor_add_node)(benchmark::State & st)
{
  add_node<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutorSimple, multi_thread_executor_remove_node)(benchmark::State & st)
{
  remove_node<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutorSimple, cbg_executor_add_node)(benchmark::State & st)
{
  add_node<rclcpp::executors::CBGExecutor>(st);
}

BENCHMARK_F(PerformanceTestExecutorSimple, cbg_executor_remove_node)(benchmark::State & st)
{
  remove_node<rclcpp::executors::CBGExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  static_single_thread_executor_add_node)(benchmark::State & st)
{
  add_node<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  static_single_thread_executor_remove_node)(benchmark::State & st)
{
  remove_node<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  single_thread_executor_spin_node_until_future_complete)(benchmark::State & st)
{
  spin_node_until_future_complete<rclcpp::executors::SingleThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  multi_thread_executor_spin_node_until_future_complete)(benchmark::State & st)
{
  spin_node_until_future_complete<rclcpp::executors::MultiThreadedExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  cbg_executor_spin_node_until_future_complete)(benchmark::State & st)
{
  spin_node_until_future_complete<rclcpp::executors::CBGExecutor>(st);
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  static_single_thread_executor_spin_node_until_future_complete)(benchmark::State & st)
{
  spin_node_until_future_complete<rclcpp::executors::StaticSingleThreadedExecutor>(st);
}

class SharedPtrHolder : public PerformanceTest
{
public:
  void SetUp(benchmark::State & st)
  {
    shared_ptr = std::make_shared<std::string>("foo");
    weak_ptr = shared_ptr;
    PerformanceTest::SetUp(st);
  }
  void TearDown(benchmark::State & st)
  {
    PerformanceTest::TearDown(st);
    shared_ptr.reset();
    weak_ptr.reset();
  }
  std::shared_ptr<std::string> shared_ptr;
  std::weak_ptr<std::string> weak_ptr;
};


BENCHMARK_F(SharedPtrHolder,
  shared_ptr_use_cnt)(benchmark::State & st)
{
    for (auto _ : st) {
    (void)_;
      size_t cnt = shared_ptr.use_count();
      benchmark::DoNotOptimize(cnt);
    }
}

BENCHMARK_F(SharedPtrHolder,
  weakt_upcast)(benchmark::State & st)
{
    for (auto _ : st) {
    (void)_;

      std::shared_ptr<std::string> ptr = weak_ptr.lock();
      benchmark::DoNotOptimize(ptr);

    }
}

class Foo
{
public:
  Foo(size_t i) : num(i) {};
  Foo() : num(0) {};

  ~Foo()
  {
  }

  size_t num = 0;
  std::shared_ptr<size_t> foo_ptr;
  rclcpp::AnyExecutable exec;
};

BENCHMARK_F(SharedPtrHolder,
  clear_std_vector)(benchmark::State & st)
{
    const size_t size = 80;

    std::vector<Foo> tmp;
    tmp.reserve(size);

    for (auto _ : st) {
    (void)_;
      tmp.clear();
      tmp.reserve(size);

      for(size_t i = 0; i < size; i++ )
      {
        tmp.push_back(i);
      }

      benchmark::DoNotOptimize(tmp);
    }
}

BENCHMARK_F(SharedPtrHolder,
  resize_zero_std_vector)(benchmark::State & st)
{
    const size_t size = 80;

    std::vector<Foo> tmp;
    tmp.reserve(size);

    for (auto _ : st) {
    (void)_;
      tmp.resize(0);
      tmp.reserve(size);

      for(size_t i = 0; i < size; i++ )
      {
        tmp.push_back(i);
      }

      benchmark::DoNotOptimize(tmp);
    }
}

using any_exec = std::variant<const rclcpp::SubscriptionBase::WeakPtr, const rclcpp::TimerBase::WeakPtr,
    const rclcpp::ServiceBase::WeakPtr, const rclcpp::ClientBase::WeakPtr,
    const rclcpp::Waitable::WeakPtr, const rclcpp::GuardCondition::WeakPtr>;

struct StupidVariant
{
  enum type {
    Subscription,
    Service,
    Timer,
    Client,
    Waitable,
    GuardCondition,
  };

  type type;

  std::weak_ptr<void> type_ptr;

  StupidVariant(rclcpp::SubscriptionBase::WeakPtr ptr) :
    type(Subscription),
    type_ptr(std::move(ptr))
  {
  }

  StupidVariant(rclcpp::TimerBase::WeakPtr ptr) :
    type(Timer),
    type_ptr(std::move(ptr))
  {
  }

  template <class Executable>
  std::shared_ptr<Executable> as_share_ptr() const
  {
    return std::static_pointer_cast<Executable>(type_ptr.lock());
  }

};

BENCHMARK_F(SharedPtrHolder,
  variant_insert)(benchmark::State & st)
{
    rclcpp::init(0, nullptr);

    rclcpp::Node node("FooNode");

    const size_t size = 80;

    std::vector<any_exec> tmp;

    std::vector<rclcpp::SubscriptionBase::SharedPtr> ptrs;
    ptrs.reserve(size);
    for(size_t i = 0; i < size; i++ )
    {
       ptrs.emplace_back(node.create_subscription<test_msgs::msg::Empty>(
              "/foo" + std::to_string(i), rclcpp::QoS(10), [] (const test_msgs::msg::Empty &/*msg*/) {}));
    }

    for (auto _ : st) {
    (void)_;
      tmp.clear();
      tmp.reserve(size);

      for(size_t i = 0; i < size; i++ )
      {
        tmp.emplace_back(ptrs[i]);
      }
    }
    rclcpp::shutdown();
}

BENCHMARK_F(SharedPtrHolder,
  push_ref)(benchmark::State & st)
{
    rclcpp::init(0, nullptr);

    rclcpp::Node node("FooNode");

    const size_t size = 80;

    std::vector<any_exec> execs;

    std::vector<rclcpp::SubscriptionBase::SharedPtr> ptrs;
    ptrs.reserve(size);
    for(size_t i = 0; i < size; i++ )
    {
       ptrs.emplace_back(node.create_subscription<test_msgs::msg::Empty>(
              "/foo" + std::to_string(i), rclcpp::QoS(10), [] (const test_msgs::msg::Empty &/*msg*/) {}));
    }
    execs.clear();
    execs.reserve(size);

    for(size_t i = 0; i < size; i++ )
    {
        execs.emplace_back(ptrs[i]);
    }
    std::vector<rclcpp::executors::AnyRef> subscribers;

    for (auto _ : st) {
    (void)_;

      subscribers.clear();
      subscribers.reserve(size);
      for(size_t i = 0; i < size; i++ )
      {
        auto handle_shr_ptr = ptrs[i]->get_subscription_handle();

//         subscribers.push_back(rclcpp::executors::SubscriberRef(ptrs[i], handle_shr_ptr , nullptr));

      }
    }

    rclcpp::shutdown();
}

BENCHMARK_F(SharedPtrHolder,
  emplace_ref)(benchmark::State & st)
{
    rclcpp::init(0, nullptr);

    rclcpp::Node node("FooNode");

    const size_t size = 80;

    std::vector<any_exec> execs;

    std::vector<rclcpp::SubscriptionBase::SharedPtr> ptrs;
    ptrs.reserve(size);
    for(size_t i = 0; i < size; i++ )
    {
       ptrs.emplace_back(node.create_subscription<test_msgs::msg::Empty>(
              "/foo" + std::to_string(i), rclcpp::QoS(10), [] (const test_msgs::msg::Empty &/*msg*/) {}));
    }
    execs.clear();
    execs.reserve(size);

    for(size_t i = 0; i < size; i++ )
    {
        execs.emplace_back(ptrs[i]);
    }
    std::vector<rclcpp::executors::AnyRef> subscribers;

    for (auto _ : st) {
    (void)_;

      subscribers.clear();
      subscribers.reserve(size);
      for(size_t i = 0; i < size; i++ )
      {
        auto handle_shr_ptr = ptrs[i]->get_subscription_handle();

//         subscribers.emplace_back(rclcpp::executors::SubscriberRef(ptrs[i], handle_shr_ptr , nullptr));

      }
    }

    rclcpp::shutdown();
}
BENCHMARK_F(SharedPtrHolder,
  switch_and_cast_insert)(benchmark::State & st)
{
    rclcpp::init(0, nullptr);

    rclcpp::Node node("FooNode");

    const size_t size = 80;

    std::vector<StupidVariant> tmp;

    std::vector<rclcpp::SubscriptionBase::SharedPtr> ptrs;
    ptrs.reserve(size);
    for(size_t i = 0; i < size; i++ )
    {
       ptrs.emplace_back(node.create_subscription<test_msgs::msg::Empty>(
              "/foo" + std::to_string(i), rclcpp::QoS(10), [] (const test_msgs::msg::Empty &/*msg*/) {}));
    }

    for (auto _ : st) {
    (void)_;
      tmp.clear();
      tmp.reserve(size);

      for(size_t i = 0; i < size; i++ )
      {
        tmp.emplace_back(ptrs[i]);
      }
    }
    rclcpp::shutdown();
}

BENCHMARK_F(SharedPtrHolder,
  stupid_variant_get)(benchmark::State & st)
{
    rclcpp::init(0, nullptr);

    rclcpp::Node node("FooNode");

    const size_t size = 80;

    std::vector<StupidVariant> execs;

    std::vector<rclcpp::SubscriptionBase::SharedPtr> ptrs;
    ptrs.reserve(size);
    for(size_t i = 0; i < size; i++ )
    {
       ptrs.emplace_back(node.create_subscription<test_msgs::msg::Empty>(
              "/foo" + std::to_string(i), rclcpp::QoS(10), [] (const test_msgs::msg::Empty &/*msg*/) {}));
    }
    execs.clear();
    execs.reserve(size);

    for(size_t i = 0; i < size; i++ )
    {
        execs.emplace_back(ptrs[i]);
    }




    for (auto _ : st) {
    (void)_;
      for(size_t i = 0; i < size; i++ )
      {

        const rclcpp::SubscriptionBase::SharedPtr ref = execs[i].as_share_ptr<rclcpp::SubscriptionBase>();

        benchmark::DoNotOptimize(ref);

      }
    }

    rclcpp::shutdown();
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  static_single_thread_executor_spin_until_future_complete)(benchmark::State & st)
{
  rclcpp::executors::StaticSingleThreadedExecutor executor;
  // test success of an immediately finishing future
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);
  auto shared_future = future.share();

  auto ret = executor.spin_until_future_complete(shared_future, 100ms);
  if (ret != rclcpp::FutureReturnCode::SUCCESS) {
    st.SkipWithError(rcutils_get_error_string().str);
  }

  reset_heap_counters();

  for (auto _ : st) {
    (void)_;
    // static_single_thread_executor has a special design. We need to add/remove the node each
    // time you call spin
    st.PauseTiming();
    executor.add_node(node);
    st.ResumeTiming();

    ret = executor.spin_until_future_complete(shared_future, 100ms);
    if (ret != rclcpp::FutureReturnCode::SUCCESS) {
      st.SkipWithError(rcutils_get_error_string().str);
      break;
    }
    st.PauseTiming();
    executor.remove_node(node);
    st.ResumeTiming();
  }
}

BENCHMARK_F(PerformanceTestExecutorSimple, spin_until_future_complete)(benchmark::State & st)
{
  // test success of an immediately finishing future
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  promise.set_value(true);
  auto shared_future = future.share();

  auto ret = rclcpp::spin_until_future_complete(node, shared_future, 1s);
  if (ret != rclcpp::FutureReturnCode::SUCCESS) {
    st.SkipWithError(rcutils_get_error_string().str);
  }

  reset_heap_counters();

  for (auto _ : st) {
    (void)_;
    ret = rclcpp::spin_until_future_complete(node, shared_future, 1s);
    if (ret != rclcpp::FutureReturnCode::SUCCESS) {
      st.SkipWithError(rcutils_get_error_string().str);
      break;
    }
  }
}

BENCHMARK_F(
  PerformanceTestExecutorSimple,
  static_executor_entities_collector_execute)(benchmark::State & st)
{
  rclcpp::executors::StaticExecutorEntitiesCollector::SharedPtr entities_collector_ =
    std::make_shared<rclcpp::executors::StaticExecutorEntitiesCollector>();
  entities_collector_->add_node(node->get_node_base_interface());

  rcl_wait_set_t wait_set = rcl_get_zero_initialized_wait_set();
  rcl_allocator_t allocator = rcl_get_default_allocator();
  auto shared_context = node->get_node_base_interface()->get_context();
  rcl_context_t * context = shared_context->get_rcl_context().get();
  rcl_ret_t ret = rcl_wait_set_init(&wait_set, 100, 100, 100, 100, 100, 100, context, allocator);
  if (ret != RCL_RET_OK) {
    st.SkipWithError(rcutils_get_error_string().str);
  }
  RCPPUTILS_SCOPE_EXIT(
  {
    rcl_ret_t ret = rcl_wait_set_fini(&wait_set);
    if (ret != RCL_RET_OK) {
      st.SkipWithError(rcutils_get_error_string().str);
    }
  });

  auto memory_strategy = rclcpp::memory_strategies::create_default_strategy();
  rclcpp::GuardCondition guard_condition(shared_context);

  entities_collector_->init(&wait_set, memory_strategy);
  RCPPUTILS_SCOPE_EXIT(entities_collector_->fini());

  reset_heap_counters();

  for (auto _ : st) {
    (void)_;
    std::shared_ptr<void> data = entities_collector_->take_data();
    entities_collector_->execute(data);
  }
}
