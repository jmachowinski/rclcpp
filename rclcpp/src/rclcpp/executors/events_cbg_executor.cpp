#include <rclcpp/executors/events_cbg_executor.hpp>


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
#include <inttypes.h>
#include "detail/callback_group_scheduler.hpp"
// #include "rclcpp/executors/detail/any_executable_weak_ref.hpp"

namespace rclcpp::executors
{

template <class ExecutableType_T>
struct WeakExecutableWithScheduler {
    WeakExecutableWithScheduler ( const std::shared_ptr<ExecutableType_T> &executable, CallbackGroupScheduler *scheduler )
        : executable ( executable ), scheduler ( scheduler )
    {
    }

    using ExecutableType = ExecutableType_T;

//  WeakExecutableWithRclHandle(WeakExecutableWithRclHandle &&) = default;
    /*
     WeakExecutableWithRclHandle& operator= (const WeakExecutableWithRclHandle &) = delete;*/


    std::weak_ptr<ExecutableType> executable;
    CallbackGroupScheduler *scheduler;
    bool processed;

    bool executable_alive()
    {
        auto use_cnt = executable.use_count();
        if ( use_cnt == 0 ) {
            return false;
        }

        return true;
    }
};

using WeakTimerRef = WeakExecutableWithScheduler<rclcpp::TimerBase>;

using WeakSubscriberRef = WeakExecutableWithScheduler<rclcpp::SubscriptionBase>;
using WeakClientRef = WeakExecutableWithScheduler<rclcpp::ClientBase>;
using WeakServiceRef = WeakExecutableWithScheduler<rclcpp::ServiceBase>;
using WeakWaitableRef = WeakExecutableWithScheduler<rclcpp::Waitable>;

struct TimerQueue2 {
    CallbackGroupSchedulerEv &scheduler;

    using Timer = std::shared_ptr<const rcl_timer_t>;

    rcl_clock_type_e timer_type;

    TimerQueue2(rcl_clock_type_e timer_type, CallbackGroupSchedulerEv &scheduler) :
        scheduler(scheduler),
        timer_type(timer_type),
        used_clock_for_timers(timer_type)
    {
    };

    rclcpp::Clock used_clock_for_timers;

    std::vector<Timer> all_timers;

    std::multimap<std::chrono::nanoseconds, Timer> running_timers;

    /**
     * Iterate over all timers, and check if we need to add one to the running list
     *
     */
    void check_all_timers()
    {
    }

    void add_timer ( const rclcpp::TimerBase::SharedPtr &timer )
    {
        rcl_clock_t *clock_type_of_timer;
        if(rcl_timer_clock(const_cast<rcl_timer_t *>(timer->get_timer_handle().get()), &clock_type_of_timer) != RCL_RET_OK)
        {
            assert(false);
        };

        if(clock_type_of_timer->type != used_clock_for_timers.get_clock_type())
        {
            // timer is handled by another queue
            return;
        }



        if(timer->get_timer_handle()->impl

        all_timers.emplace_back ( timer->get_timer_handle() ); //, &scheduler);

        if ( !timer->is_canceled() ) {
            //FIXME std::chrono::steady_clock::now() is wrong here
            auto next_trigger_time = std::chrono::steady_clock::now() + timer->time_until_trigger();

//             running_timers.emplace ( next_trigger_time, timer->get_timer_handle() );
        }

        timer->set_on_reset_callback ( [this] ( size_t ) {
            check_all_timers();
        } );
    };

    /**
     * Returns the estimated time until the next timer wakes up
     */
    std::chrono::nanoseconds get_min_sleep_time() const
    {
    }

    Timer *get_next_ready_timer();

};


struct TimerQueue {
    CallbackGroupSchedulerEv &scheduler;

    using Timer = std::shared_ptr<const rcl_timer_t>;

//       RCL_ROS_TIME,
//   /// Use system time
//   RCL_SYSTEM_TIME,
//   /// Use a steady clock time
//   RCL_STEADY_TIME

    std::vector<Timer> system_time_timers;
    std::vector<Timer> steady_time_timers;
    std::vector<Timer> ros_time_timers;

    std::vector<Timer> all_timers;

    std::multimap<std::chrono::nanoseconds, Timer> running_timers;

    /**
     * Iterate over all timers, and check if we need to add one to the running list
     *
     */
    void check_all_timers()
    {
    }

    void add_timer ( const rclcpp::TimerBase::SharedPtr &timer )
    {
        all_timers.emplace_back ( timer->get_timer_handle() ); //, &scheduler);

        if ( !timer->is_canceled() ) {
            //FIXME std::chrono::steady_clock::now() is wrong here
            auto next_trigger_time = std::chrono::steady_clock::now() + timer->time_until_trigger();

//             running_timers.emplace ( next_trigger_time, timer->get_timer_handle() );
        }

        timer->set_on_reset_callback ( [this] ( size_t ) {
            check_all_timers();
        } );
    };

    /**
     * Returns the estimated time until the next timer wakes up
     */
    std::chrono::nanoseconds get_min_sleep_time() const
    {
    }

    Timer *get_next_ready_timer();

};

struct WeakExecutableCache {
    WeakExecutableCache ( CallbackGroupSchedulerEv &scheduler ) :
        scheduler ( scheduler )
    {
    }

    WeakExecutableCache();

    std::vector<WeakTimerRef> timers;
    std::vector<WeakSubscriberRef> subscribers;
    std::vector<WeakClientRef> clients;
    std::vector<WeakServiceRef> services;
    std::vector<WeakWaitableRef> waitables;
    std::vector<GuardConditionWithFunction> guard_conditions;

    CallbackGroupSchedulerEv &scheduler;
    std::unique_ptr<rclcpp::experimental::TimersManager> timer_manager;

    bool cache_ditry = true;

    void clear()
    {
        timers.clear();
        subscribers.clear();
        clients.clear();
        services.clear();
        waitables.clear();
        guard_conditions.clear();
    }


    void regenerate_events ( rclcpp::CallbackGroup & callback_group )
    {
        clear();

        timer_manager->clear();

        // we reserve to much memory here, this this should be fine
        if ( timers.capacity() == 0 ) {
            timers.reserve ( callback_group.size() );
            subscribers.reserve ( callback_group.size() );
            clients.reserve ( callback_group.size() );
            services.reserve ( callback_group.size() );
            waitables.reserve ( callback_group.size() );
        }

        const auto add_sub = [this] ( const rclcpp::SubscriptionBase::SharedPtr &s ) {
            //element not found, add new one
//       auto &entry = subscribers.emplace_back(WeakSubscriberRef(s, &scheduler));
            s->set_on_new_message_callback ( [weak_ptr = rclcpp::SubscriptionBase::WeakPtr ( s ), this] ( size_t nr_msg ) mutable {
                for ( size_t i = 0; i < nr_msg; i++ )
                {
                    scheduler.add_ready_executable ( weak_ptr );
                }
            }
                                           );
        };
        const auto add_timer = [this] ( const rclcpp::TimerBase::SharedPtr &s ) {
            timer_manager->add_timer ( s, [weak_ptr = rclcpp::TimerBase::WeakPtr ( s ), this] ( const TimerBase * ) mutable {
                scheduler.add_ready_executable ( weak_ptr );
            } );
        };

        const auto add_client = [this] ( const rclcpp::ClientBase::SharedPtr &s ) {
            s->set_on_new_response_callback ( [weak_ptr = rclcpp::ClientBase::WeakPtr ( s ), this] ( size_t nr_msg ) mutable {
                for ( size_t i = 0; i < nr_msg; i++ )
                {
                    scheduler.add_ready_executable ( weak_ptr );
                }
            }
                                            );
        };

        const auto add_service = [this] ( const rclcpp::ServiceBase::SharedPtr &s ) {
            s->set_on_new_request_callback ( [weak_ptr = rclcpp::ServiceBase::WeakPtr ( s ), this] ( size_t nr_msg ) mutable {
                for ( size_t i = 0; i < nr_msg; i++ )
                {
                    scheduler.add_ready_executable ( weak_ptr );
                }
            }
                                           );
        };


        add_guard_condition_event ( callback_group.get_notify_guard_condition(), [this]() {
//         RCUTILS_LOG_ERROR_NAMED("rclcpp", "GC: Callback group was changed");

            cache_ditry = true;
        } );

        const auto add_waitable = [this] ( const rclcpp::Waitable::SharedPtr &s ) {
            s->set_on_ready_callback ( [weak_ptr = rclcpp::Waitable::WeakPtr ( s ), this] ( size_t nr_msg, int internal_ev_type ) mutable {
                for ( size_t i = 0; i < nr_msg; i++ )
                {
                    scheduler.add_ready_executable ( WaitableWithEventType{weak_ptr, internal_ev_type} );
                }
            }
                                     );
        };


        callback_group.collect_all_ptrs ( add_sub, add_service, add_client, add_timer, add_waitable );

//     RCUTILS_LOG_ERROR_NAMED("rclcpp", "GC: regeneraetd, new size : t %lu, sub %lu, c %lu, s %lu, gc %lu, waitables %lu", wait_set_size.timers, wait_set_size.subscriptions, wait_set_size.clients, wait_set_size.services, wait_set_size.guard_conditions, waitables.size());


        cache_ditry = false;
    }



    void add_guard_condition_event ( rclcpp::GuardCondition::SharedPtr ptr, std::function<void ( void ) > fun )
    {
        guard_conditions.emplace_back ( GuardConditionWithFunction ( ptr, std::move ( fun ) ) );


        for ( auto &entry : guard_conditions ) {
            entry.guard_condition->set_on_trigger_callback ( [ptr = &entry] ( size_t nr_events ) {
                for ( size_t i = 0; i < nr_events; i++ ) {
                    // FIXME add to scheduler
                }
            } );
        }

    }
};


EventsCBGExecutor::EventsCBGExecutor (
    const rclcpp::ExecutorOptions & options,
    size_t number_of_threads,
    std::chrono::nanoseconds next_exec_timeout )
    :
    next_exec_timeout_ ( next_exec_timeout ),
    spinning ( false ),
    interrupt_guard_condition_ ( std::make_shared<rclcpp::GuardCondition> ( options.context ) ),
    shutdown_guard_condition_ ( std::make_shared<rclcpp::GuardCondition> ( options.context ) ),
    context_ ( options.context ),
    global_executable_cache ( std::make_unique<WeakExecutableWithRclHandleCache>() ),
    nodes_executable_cache ( std::make_unique<WeakExecutableWithRclHandleCache>() )
{

    global_executable_cache->add_guard_condition (
        interrupt_guard_condition_,
        std::function<void ( void ) >() );
    global_executable_cache->add_guard_condition (
    shutdown_guard_condition_, [this] () {
        work_ready_conditional.notify_all();
    } );


    number_of_threads_ = number_of_threads > 0 ?
                         number_of_threads :
                         std::max ( std::thread::hardware_concurrency(), 2U );

    shutdown_callback_handle_ = context_->add_on_shutdown_callback (
    [weak_gc = std::weak_ptr<rclcpp::GuardCondition> {shutdown_guard_condition_}]() {
        auto strong_gc = weak_gc.lock();
        if ( strong_gc ) {
            strong_gc->trigger();
        }
    } );

    rcl_allocator_t allocator = options.memory_strategy->get_allocator();

    rcl_ret_t ret = rcl_wait_set_init (
                        &wait_set_,
                        0, 0, 0, 0, 0, 0,
                        context_->get_rcl_context().get(),
                        allocator );
    if ( RCL_RET_OK != ret ) {
        RCUTILS_LOG_ERROR_NAMED (
            "rclcpp",
            "failed to create wait set: %s", rcl_get_error_string().str );
        rcl_reset_error();
        exceptions::throw_from_rcl_error ( ret, "Failed to create wait set in Executor constructor" );
    }

}

EventsCBGExecutor::~EventsCBGExecutor()
{

    work_ready_conditional.notify_all();

    std::vector<node_interfaces::NodeBaseInterface::WeakPtr> added_nodes_cpy;
    {
        std::lock_guard lock{added_nodes_mutex_};
        added_nodes_cpy = added_nodes;
    }

    for ( const node_interfaces::NodeBaseInterface::WeakPtr & node_weak_ptr : added_nodes_cpy ) {
        const node_interfaces::NodeBaseInterface::SharedPtr & node_ptr = node_weak_ptr.lock();
        if ( node_ptr ) {
            remove_node ( node_ptr, false );
        }
    }

    std::vector<rclcpp::CallbackGroup::WeakPtr> added_cbgs_cpy;
    {
        std::lock_guard lock{added_callback_groups_mutex_};
        added_cbgs_cpy = added_callback_groups;
    }

    for ( const auto & weak_ptr : added_cbgs_cpy ) {
        auto shr_ptr = weak_ptr.lock();
        if ( shr_ptr ) {
            remove_callback_group ( shr_ptr, false );
        }
    }

    // Remove shutdown callback handle registered to Context
    if ( !context_->remove_on_shutdown_callback ( shutdown_callback_handle_ ) ) {
        RCUTILS_LOG_ERROR_NAMED (
            "rclcpp",
            "failed to remove registered on_shutdown callback" );
        rcl_reset_error();
    }

}

bool EventsCBGExecutor::execute_ready_executables_until ( const std::chrono::time_point<std::chrono::steady_clock> &stop_time )
{
    bool found_work = false;

    for ( size_t i = CallbackGroupSchedulerEv::Priorities::Timer;
            i <= CallbackGroupSchedulerEv::Priorities::Waitable; i++ ) {
        CallbackGroupSchedulerEv::Priorities cur_prio ( static_cast<CallbackGroupSchedulerEv::Priorities> ( i ) );

        for ( CallbackGroupData & cbg_with_data : callback_groups ) {
            if ( cbg_with_data.scheduler->execute_unprocessed_executable_until ( stop_time, cur_prio ) ) {
                if ( std::chrono::steady_clock::now() >= stop_time ) {
                    return true;
                }
            }
        }
    }
    return found_work;
}


bool EventsCBGExecutor::get_next_ready_executable ( AnyExecutableCbgEv & any_executable )
{
    if ( !spinning.load() ) {
        return false;
    }

    std::lock_guard g ( callback_groups_mutex );
//     RCUTILS_LOG_ERROR_NAMED("rclcpp", "get_next_ready_executable");

    struct ReadyCallbacksWithSharedPtr {
        CallbackGroupData * data;
        rclcpp::CallbackGroup::SharedPtr callback_group;
        bool ready = true;
    };

    std::vector<ReadyCallbacksWithSharedPtr> ready_callbacks;
    ready_callbacks.reserve ( callback_groups.size() );


    for ( auto it = callback_groups.begin(); it != callback_groups.end(); ) {
        CallbackGroupData & cbg_with_data ( *it );

        ReadyCallbacksWithSharedPtr e;
        e.callback_group = cbg_with_data.callback_group.lock();
        if ( !e.callback_group ) {
            it = callback_groups.erase ( it );
            continue;
        }

        if ( e.callback_group->can_be_taken_from().load() ) {
            e.data = &cbg_with_data;
            ready_callbacks.push_back ( std::move ( e ) );
        }

        it++;
    }

    bool found_work = false;

    for ( size_t i = CallbackGroupSchedulerEv::Priorities::Timer;
            i <= CallbackGroupSchedulerEv::Priorities::Waitable; i++ ) {
        CallbackGroupSchedulerEv::Priorities cur_prio ( static_cast<CallbackGroupSchedulerEv::Priorities> ( i ) );
        for ( ReadyCallbacksWithSharedPtr & ready_elem: ready_callbacks ) {

            if ( !found_work ) {
                if ( ready_elem.data->scheduler->get_unprocessed_executable ( any_executable, cur_prio ) ) {
                    // mark callback group as in use
                    ready_elem.callback_group->can_be_taken_from().store ( false );
                    found_work = true;
                    ready_elem.ready = false;
//                     RCUTILS_LOG_ERROR_NAMED("rclcpp", "get_next_ready_executable : found ready executable");
                }
            } else {
                if ( ready_elem.ready && ready_elem.data->scheduler->has_unprocessed_executables() ) {
                    //wake up worker thread
                    work_ready_conditional.notify_one();
                    return true;
                }
            }
        }
    }

    return found_work;
}

size_t
EventsCBGExecutor::get_number_of_threads()
{
    return number_of_threads_;
}

void EventsCBGExecutor::sync_callback_groups()
{
    if ( !needs_callback_group_resync.exchange ( false ) ) {
        return;
    }
//   RCUTILS_LOG_ERROR_NAMED("rclcpp", "sync_callback_groups");

    std::vector<std::pair<CallbackGroupData *, rclcpp::CallbackGroup::SharedPtr>> cur_group_data;
    cur_group_data.reserve ( callback_groups.size() );

    for ( CallbackGroupData & d : callback_groups ) {
        auto p = d.callback_group.lock();
        if ( p ) {
            cur_group_data.emplace_back ( &d, std::move ( p ) );
        }
    }

    std::scoped_lock<std::mutex> lk ( callback_groups_mutex );
    std::vector<CallbackGroupData> next_group_data;

    std::set<CallbackGroup *> added_cbgs;

    auto insert_data = [&cur_group_data, &next_group_data, &added_cbgs] ( rclcpp::CallbackGroup::SharedPtr &&cbg ) {
        // nodes may share callback groups, therefore we need to make sure we only add them once
        if ( added_cbgs.find ( cbg.get() ) != added_cbgs.end() ) {
            return;
        }

        added_cbgs.insert ( cbg.get() );

        for ( const auto & pair : cur_group_data ) {
            if ( pair.second == cbg ) {
//                 RCUTILS_LOG_INFO("Using existing callback group");
                next_group_data.push_back ( std::move ( *pair.first ) );
                return;
            }
        }

//         RCUTILS_LOG_INFO("Using new callback group");

        CallbackGroupData new_entry;
        new_entry.scheduler = std::make_unique<CallbackGroupSchedulerEv>();
        new_entry.executable_cache = std::make_unique<WeakExecutableCache> ( *new_entry.scheduler );
        new_entry.executable_cache->regenerate_events ( *cbg );
        new_entry.callback_group = std::move ( cbg );
        next_group_data.push_back ( std::move ( new_entry ) );
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

        // *3 ist a rough estimate of how many callback_group a node may have
        next_group_data.reserve ( added_cbgs_cpy.size() + added_nodes_cpy.size() * 3 );

        nodes_executable_cache->clear();
//         nodes_executable_cache->guard_conditions.reserve(added_nodes_cpy.size());

//         RCUTILS_LOG_INFO("Added node size is %lu", added_nodes_cpy.size());

        for ( const node_interfaces::NodeBaseInterface::WeakPtr & node_weak_ptr : added_nodes_cpy ) {
            auto node_ptr = node_weak_ptr.lock();
            if ( node_ptr ) {
                node_ptr->for_each_callback_group (
                [&insert_data] ( rclcpp::CallbackGroup::SharedPtr cbg ) {
                    if ( cbg->automatically_add_to_executor_with_node() ) {
                        insert_data ( std::move ( cbg ) );
                    }
                } );

                // register node guard condition, and trigger resync on node change event
                nodes_executable_cache->add_guard_condition ( node_ptr->get_shared_notify_guard_condition(),
                [this] () {
//                                     RCUTILS_LOG_INFO("Node changed GC triggered");
                    needs_callback_group_resync.store ( true );
                } );
            }
        }

        for ( const rclcpp::CallbackGroup::WeakPtr & cbg : added_cbgs_cpy ) {
            auto p = cbg.lock();
            if ( p ) {
                insert_data ( std::move ( p ) );
            }
        }
    }

    callback_groups.swap ( next_group_data );
}


void EventsCBGExecutor::do_housekeeping()
{
    using namespace rclcpp::exceptions;

    sync_callback_groups();

    {
        std::lock_guard g ( callback_groups_mutex );
        for ( CallbackGroupData & cbg_with_data: callback_groups ) {

            // don't add anything to a waitset that has unprocessed data
            if ( cbg_with_data.scheduler->has_unprocessed_executables() ) {
                continue;
            }

            auto cbg_shr_ptr = cbg_with_data.callback_group.lock();
            if ( !cbg_shr_ptr ) {
                continue;
            }

            if ( !cbg_shr_ptr->can_be_taken_from() ) {
                continue;
            }

            //     CallbackGroupState & cbg_state = *cbg_with_data.callback_group_state;
            // regenerate the state data
            if ( cbg_with_data.executable_cache->cache_ditry ) {
                //       RCUTILS_LOG_INFO("Regenerating callback group");
                // Regenerating clears the dirty flag
                cbg_with_data.executable_cache->regenerate_events ( *cbg_shr_ptr );
            }
        }
    }

}

void
EventsCBGExecutor::run ( size_t this_thread_number )
{
    ( void ) this_thread_number;

    while ( rclcpp::ok ( this->context_ ) && spinning.load() ) {
        rclcpp::executors::AnyExecutableCbgEv any_exec;

        if ( !get_next_ready_executable ( any_exec ) ) {

            if ( spinning.load() ) {
                std::unique_lock lk ( conditional_mutex );
//                 RCUTILS_LOG_ERROR_NAMED("rclcpp", "going to sleep");
                work_ready_conditional.wait ( lk );
            }
//                 RCUTILS_LOG_ERROR_NAMED("rclcpp", "woken up");
            continue;
        }

        any_exec.execute_function();
    }

//     RCUTILS_LOG_INFO("Stopping execution thread");
}

void EventsCBGExecutor::spin_once_internal ( std::chrono::nanoseconds timeout )
{
    AnyExecutableCbgEv any_exec;
    {
        if ( !rclcpp::ok ( this->context_ ) || !spinning.load() ) {
            return;
        }

        if ( !get_next_ready_executable ( any_exec ) ) {

            std::unique_lock lk ( conditional_mutex );
            work_ready_conditional.wait_for ( lk, timeout );

            if ( !get_next_ready_executable ( any_exec ) ) {
                return;
            }
        }
    }

    any_exec.execute_function();
}

void
EventsCBGExecutor::spin_once ( std::chrono::nanoseconds timeout )
{
    if ( spinning.exchange ( true ) ) {
        throw std::runtime_error ( "spin_once() called while already spinning" );
    }
    RCPPUTILS_SCOPE_EXIT ( this->spinning.store ( false ); );

    spin_once_internal ( timeout );
}


void
EventsCBGExecutor::spin_some ( std::chrono::nanoseconds max_duration )
{
    collect_and_execute_ready_events ( max_duration, false );
}

void EventsCBGExecutor::spin_all ( std::chrono::nanoseconds max_duration )
{
    if ( max_duration < std::chrono::nanoseconds::zero() ) {
        throw std::invalid_argument ( "max_duration must be greater than or equal to 0" );
    }

    collect_and_execute_ready_events ( max_duration, true );
}

bool EventsCBGExecutor::collect_and_execute_ready_events (
    std::chrono::nanoseconds max_duration,
        bool recollect_if_no_work_available )
{
    if ( spinning.exchange ( true ) ) {
        throw std::runtime_error ( "collect_and_execute_ready_events() called while already spinning" );
    }
    RCPPUTILS_SCOPE_EXIT ( this->spinning.store ( false ); );

    const auto start = std::chrono::steady_clock::now();
    const auto end_time = start + max_duration;
    auto cur_time = start;

    // collect any work, that is already ready
//   wait_for_work(std::chrono::nanoseconds::zero(), true);

    bool got_work_since_collect = false;
    bool first_collect = true;
    bool had_work = false;

    //FIXME this is super hard to do, we need to know when to stop

    while ( rclcpp::ok ( this->context_ ) && spinning && cur_time < end_time ) {
        if ( !execute_ready_executables_until ( end_time ) ) {

            if ( !first_collect && !recollect_if_no_work_available ) {
                // we are done
                return had_work;
            }

            if ( first_collect || got_work_since_collect ) {
                // wait for new work

//                 std::unique_lock lk(conditional_mutex);
//                 work_ready_conditional.wait_for(lk, std::chrono::nanoseconds::zero());
//                 wait_for_work(std::chrono::nanoseconds::zero(), !first_collect);

                first_collect = false;
                got_work_since_collect = false;
                continue;
            }

            return had_work;
        } else {
            got_work_since_collect = true;
        }

        cur_time = std::chrono::steady_clock::now();
    }

    return had_work;
}
void
EventsCBGExecutor::spin()
{
//     RCUTILS_LOG_ERROR_NAMED(
//         "rclcpp",
//         "EventsCBGExecutor::spin()");


    if ( spinning.exchange ( true ) ) {
        throw std::runtime_error ( "spin() called while already spinning" );
    }
    RCPPUTILS_SCOPE_EXIT ( this->spinning.store ( false ); );
    std::vector<std::thread> threads;
    size_t thread_id = 0;
    {
        std::lock_guard wait_lock{wait_mutex_};
        for ( ; thread_id < number_of_threads_ - 1; ++thread_id ) {
            auto func = std::bind ( &EventsCBGExecutor::run, this, thread_id );
            threads.emplace_back ( func );
        }
    }

    run ( thread_id );
    for ( auto & thread : threads ) {
        thread.join();
    }
}

void
EventsCBGExecutor::add_callback_group (
    rclcpp::CallbackGroup::SharedPtr group_ptr,
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr /*node_ptr*/,
    bool notify )
{
    {
        std::lock_guard lock{added_callback_groups_mutex_};
        added_callback_groups.push_back ( group_ptr );
    }
    needs_callback_group_resync = true;
    if ( notify ) {
        // Interrupt waiting to handle new node
        try {
            interrupt_guard_condition_->trigger();
        } catch ( const rclcpp::exceptions::RCLError & ex ) {
            throw std::runtime_error (
                std::string (
                    "Failed to trigger guard condition on callback group add: " ) + ex.what() );
        }
    }
}

void
EventsCBGExecutor::cancel()
{
    spinning.store ( false );

    work_ready_conditional.notify_all();

    try {
        interrupt_guard_condition_->trigger();
    } catch ( const rclcpp::exceptions::RCLError & ex ) {
        throw std::runtime_error (
            std::string ( "Failed to trigger guard condition in cancel: " ) + ex.what() );
    }
    work_ready_conditional.notify_all();
}

std::vector<rclcpp::CallbackGroup::WeakPtr>
EventsCBGExecutor::get_all_callback_groups()
{
    std::lock_guard lock{added_callback_groups_mutex_};
    return added_callback_groups;
}

void
EventsCBGExecutor::remove_callback_group (
    rclcpp::CallbackGroup::SharedPtr group_ptr,
    bool notify )
{
    bool found = false;
    {
        std::lock_guard lock{added_callback_groups_mutex_};
        added_callback_groups.erase (
            std::remove_if (
                added_callback_groups.begin(), added_callback_groups.end(),
        [&group_ptr, &found] ( const auto & weak_ptr ) {
            auto shr_ptr = weak_ptr.lock();
            if ( !shr_ptr ) {
                return true;
            }

            if ( group_ptr == shr_ptr ) {
                found = true;
                return true;
            }
            return false;
        } ), added_callback_groups.end() );
        added_callback_groups.push_back ( group_ptr );
    }

    if ( found ) {
        needs_callback_group_resync = true;
    }

    if ( notify ) {
        // Interrupt waiting to handle new node
        try {
            interrupt_guard_condition_->trigger();
        } catch ( const rclcpp::exceptions::RCLError & ex ) {
            throw std::runtime_error (
                std::string (
                    "Failed to trigger guard condition on callback group add: " ) + ex.what() );
        }
    }
}

void
EventsCBGExecutor::add_node ( rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr, bool notify )
{
    // If the node already has an executor
    std::atomic_bool & has_executor = node_ptr->get_associated_with_executor_atomic();
    if ( has_executor.exchange ( true ) ) {
        throw std::runtime_error (
            std::string ( "Node '" ) + node_ptr->get_fully_qualified_name() +
            "' has already been added to an executor." );
    }

    {
        std::lock_guard lock{added_nodes_mutex_};
        added_nodes.push_back ( node_ptr );
    }

    needs_callback_group_resync = true;

    if ( notify ) {
        // Interrupt waiting to handle new node
        try {
            interrupt_guard_condition_->trigger();
        } catch ( const rclcpp::exceptions::RCLError & ex ) {
            throw std::runtime_error (
                std::string (
                    "Failed to trigger guard condition on callback group add: " ) + ex.what() );
        }
    }
}

void
EventsCBGExecutor::add_node ( std::shared_ptr<rclcpp::Node> node_ptr, bool notify )
{
    add_node ( node_ptr->get_node_base_interface(), notify );
}

void
EventsCBGExecutor::remove_node (
    rclcpp::node_interfaces::NodeBaseInterface::SharedPtr node_ptr,
    bool notify )
{
    {
        std::lock_guard lock{added_nodes_mutex_};
        added_nodes.erase (
            std::remove_if (
        added_nodes.begin(), added_nodes.end(), [&node_ptr] ( const auto & weak_ptr ) {
            const auto shr_ptr = weak_ptr.lock();
            if ( shr_ptr && shr_ptr == node_ptr ) {
                return true;
            }
            return false;
        } ), added_nodes.end() );
    }

    needs_callback_group_resync = true;

    if ( notify ) {
        // Interrupt waiting to handle new node
        try {
            interrupt_guard_condition_->trigger();
        } catch ( const rclcpp::exceptions::RCLError & ex ) {
            throw std::runtime_error (
                std::string (
                    "Failed to trigger guard condition on callback group add: " ) + ex.what() );
        }
    }

    node_ptr->get_associated_with_executor_atomic().store ( false );
}

void
EventsCBGExecutor::remove_node ( std::shared_ptr<rclcpp::Node> node_ptr, bool notify )
{
    remove_node ( node_ptr->get_node_base_interface(), notify );
}

// add a callback group to the executor, not bound to any node
void EventsCBGExecutor::add_callback_group_only ( rclcpp::CallbackGroup::SharedPtr group_ptr )
{
    add_callback_group ( group_ptr, nullptr, true );
}
}
