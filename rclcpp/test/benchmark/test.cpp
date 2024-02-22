#include <rclcpp/rclcpp.hpp>
#include <rclcpp/executors/cbg_executor.hpp>
#include "test_msgs/msg/empty.hpp"

        class ExecuterDerived : public rclcpp::executors::CBGExecutor
    {
public:
      void call_wait_for_work(std::chrono::nanoseconds timeout)
      {
        rclcpp::executors::CBGExecutor::wait_for_work(timeout);
      }
    };


void do_test(ExecuterDerived &executor, rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr pub, rclcpp::Node::SharedPtr node)
{
    test_msgs::msg::Empty empty_msgs;

    // only need to force a waitset rebuild
    rclcpp::PublisherBase::SharedPtr somePub;

    using namespace std::chrono_literals;


    for (int i = 0; i < 100; i++) {

      somePub = node->create_publisher<test_msgs::msg::Empty>(
            "/foo", rclcpp::QoS(10));
        // we need one ready event
        pub->publish(empty_msgs);


      executor.call_wait_for_work(100ms);

      // process all events in the wait queue
      executor.spin_some(100ms);
    }
}

int main()
{
    test_msgs::msg::Empty empty_msgs;
  std::vector<rclcpp::Node::SharedPtr> nodes;
  std::vector<rclcpp::Publisher<test_msgs::msg::Empty>::SharedPtr> publishers;
  std::vector<rclcpp::Subscription<test_msgs::msg::Empty>::SharedPtr> subscriptions;
  uint callback_count;

    constexpr unsigned int kNumberOfNodes = 10;
    constexpr unsigned int kNumberOfPubSubs = 10;

        rclcpp::init(0, nullptr);
    callback_count = 0;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      nodes.push_back(std::make_shared<rclcpp::Node>("my_node_" + std::to_string(i)));

      for (unsigned int j = 0u; j < kNumberOfPubSubs; j++) {
        publishers.push_back(
          nodes[i]->create_publisher<test_msgs::msg::Empty>(
            "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10)));

        auto callback = [&](test_msgs::msg::Empty::ConstSharedPtr) {callback_count++;};
        subscriptions.push_back(
          nodes[i]->create_subscription<test_msgs::msg::Empty>(
            "/empty_msgs_" + std::to_string(i) + "_" + std::to_string(j), rclcpp::QoS(10), std::move(callback)));
      }
    }

    using namespace std::chrono_literals;

    ExecuterDerived executor;
    for (unsigned int i = 0u; i < kNumberOfNodes; i++) {
      executor.add_node(nodes[i]);
      executor.spin_some(100ms);
    }


    do_test(executor, publishers.front(), nodes.front());




        subscriptions.clear();
    publishers.clear();
    nodes.clear();
    rclcpp::shutdown();


    return 0;
}
