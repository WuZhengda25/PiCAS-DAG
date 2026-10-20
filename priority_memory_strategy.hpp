// Copyright 2015 Open Source Robotics Foundation, Inc.
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

#ifndef RTIS_PRIORITY_STRATEGY
#define RTIS_PRIORITY_STRATEGY

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <queue>
#include <time.h>

#include "rcl/allocator.h"

#include "rclcpp/allocator/allocator_common.hpp"
#include "rclcpp/memory_strategy.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/visibility_control.hpp"

#include "rcutils/logging_macros.h"

#include "rmw/types.h"

#include "simple_timer/rt-sched.hpp"

/// Delegate for handling memory allocations while the Executor is executing.
/**
 * By default, the memory strategy dynamically allocates memory for structures that come in from
 * the rmw implementation after the executor waits for work, based on the number of entities that
 * come through.
 */

enum ExecutableType
{
    SUBSCRIPTION,
    SERVICE,
    CLIENT,
    TIMER,
    WAITABLE
};

enum ExecutableScheduleType
{
    CHAIN_INDEPENDENT_PRIORITY, // not used here
    CHAIN_AWARE_PRIORITY,
    DEADLINE,
    DEFAULT, // not used here
    CHAIN_AWARE_PRIORITY_DAG,
};

class PriorityExecutable
{
public:
    std::shared_ptr<const void> handle;
    ExecutableType type;
    bool can_be_run = true;
    std::shared_ptr<rclcpp::Waitable> waitable;
    ExecutableScheduleType sched_type;

    //int priority;
    long period = 1000; // milliseconds
    long deadline = 1000;
    //double_t runtime;
    long *release_time = nullptr; // for timer_handle

    bool is_first_in_chain = false;
    bool is_last_in_chain = false;
    // chain aware deadlines
    int *cur_index = nullptr;
    std::vector<std::deque<uint> *> *deadlines = nullptr;
    std::shared_ptr<rclcpp::TimerBase> timer_handle;
    // just used for logging
    int chain_id = 0;
    int priority;
    // chain aware priority
    int counter = 0;
    // int queue_counter = 0;
    // int release_counter = 0;
    // The number of release of the chain
    int actual_collect = 0;
    long long *sum = nullptr;

    int *priority_dag = nullptr;
    //int fix_priority;  //fix priority, -1 indicates this callback is runtime callback
    PriorityExecutable* successor_callback = nullptr; 
    std::queue<int>* runtime_priority = nullptr; // nullptr indicates this callback is fixed callback
    std::unordered_map<int, std::vector<std::pair<int, PriorityExecutable*>>>* successor_callbacks = nullptr; // nullptr means this callback is sink callback or its sucessor callback is fixed callback
    int callback_id = 0;

    PriorityExecutable(std::shared_ptr<const void> h, int p, ExecutableType t, std::unordered_map<int, std::vector<std::pair<int, PriorityExecutable*>>>* mp, ExecutableScheduleType sched_type = CHAIN_AWARE_PRIORITY_DAG) {
        handle = h;
        type = t;
        if (sched_type == CHAIN_AWARE_PRIORITY_DAG) {
            priority_dag = new int(p); // priority == -1 means this callback is runtime callback
            if (*priority_dag == -1) {
                runtime_priority = new std::queue<int>();
            }
            //std::cout << "110: " << *priority_dag << std::endl;
            successor_callbacks = mp;            
        }
        // else if (sched_type == DEADLINE)
        // {
        //     period = p;
        // }
        this->sched_type = sched_type;
        this->sum = new long long(0);
        this->cur_index = new int(0);
    }
    PriorityExecutable(std::shared_ptr<const void> h, int p, ExecutableType t, ExecutableScheduleType sched_type = CHAIN_INDEPENDENT_PRIORITY)
    {
        handle = h;
        type = t;
        if (sched_type == CHAIN_INDEPENDENT_PRIORITY || sched_type == CHAIN_AWARE_PRIORITY)
        {
            priority = p;
        }
        else if (sched_type == DEADLINE)
        {
            period = p;
        }
        this->sched_type = sched_type;
        this->sum = new long long(0);
        this->cur_index = new int(0);
    }

    PriorityExecutable(std::shared_ptr<const void> h, int p, int d, ExecutableType t, ExecutableScheduleType sched_type = CHAIN_INDEPENDENT_PRIORITY)
    {
        handle = h;
        type = t;
        if (sched_type == CHAIN_INDEPENDENT_PRIORITY || sched_type == CHAIN_AWARE_PRIORITY)
        {
            priority = p;
        }
        else if (sched_type == DEADLINE)
        {
            period = p;
            deadline = d;
        }
        this->sched_type = sched_type;
        this->sum = new long long(0);
        this->cur_index = new int(0);
    }
    void dont_run()
    {
        this->can_be_run = false;
    }

    void allow_run()
    {
        if (this->can_be_run)
        {
            // release has been detected
        }
        else
        {
            this->can_be_run = true;
        }
    }

    PriorityExecutable()
    {
        handle = nullptr;
        priority = 0;
        type = SUBSCRIPTION;
        sum = new long long(0);
    }

    void increment_counter()
    {
        this->counter += 1;
    }
};

class PriorityExecutableComparator
{
public:
    bool operator()(const PriorityExecutable *p1, const PriorityExecutable *p2)
    {
        if (p1 == nullptr || p2 == nullptr)
        {
            // TODO: realistic value
            return 0;
        }
        if (p1->sched_type != p2->sched_type)
        {
            if (p1->sched_type == DEADLINE)
            {
                return false;
            }
            else if (p2->sched_type == DEADLINE)
            {
                return true;
            }
            if (p1->sched_type == CHAIN_INDEPENDENT_PRIORITY)
            {
                return false;
            }
            else if (p2->sched_type == CHAIN_INDEPENDENT_PRIORITY)
            {
                return true;
            }
            else if (p1->sched_type == CHAIN_AWARE_PRIORITY)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        if (p1->sched_type == CHAIN_INDEPENDENT_PRIORITY)
        {
            // lower value runs first
            return p1->priority > p2->priority;
        }
        if (p1->sched_type == CHAIN_AWARE_PRIORITY)
        {
            if (p1->priority != p2->priority)
            {
                return p1->priority < p2->priority;
            }
            // return p1->counter > p2->counter;
            return 0;
        }
        if (p1->sched_type == CHAIN_AWARE_PRIORITY_DAG)
        {
            if (*p1->priority_dag != *p2->priority_dag)
            {
                return *p1->priority_dag < *p2->priority_dag;
            }
            // return p1->counter > p2->counter;
            return 0;
        }
        if (p1->sched_type == DEADLINE)
        {
            // TODO: use the counter logic here as well

            uint p1_deadline = 0;
            uint p2_deadline = 0;
            if (p1->deadlines != nullptr && !(*p1->deadlines)[*p1->cur_index]->empty())
            {
                p1_deadline = (*p1->deadlines)[*p1->cur_index]->front();
            }
            if (p2->deadlines != nullptr && !(*p2->deadlines)[*p2->cur_index]->empty())
            {
                p2_deadline = (*p2->deadlines)[*p2->cur_index]->front();
            }
            if (p1_deadline == 0)
            {
                return true;
            }
            if (p2_deadline == 0)
            {
                return false;
            }
            if (p1_deadline == p2_deadline)
            {
                // these must be from the same chain
                // settle the difference with the counter
                return p1->counter > p2->counter;
            }
            return p1_deadline > p2_deadline;
        }
        else
        {
            std::cout << "invalid compare opration on priority_exec" << std::endl;
            return 0;
        }
    }
};
template <typename Alloc = std::allocator<void>>
class PriorityMemoryStrategy : public rclcpp::memory_strategy::MemoryStrategy
{
public:
    RCLCPP_SMART_PTR_DEFINITIONS(PriorityMemoryStrategy<Alloc>)

    node_time_logger logger;
    bool is_f1tenth = false;

    using VoidAllocTraits = typename rclcpp::allocator::AllocRebind<void *, Alloc>;
    using VoidAlloc = typename VoidAllocTraits::allocator_type;

    explicit PriorityMemoryStrategy(std::shared_ptr<Alloc> allocator)
    {
        allocator_ = std::make_shared<VoidAlloc>(*allocator.get());
    }

    PriorityMemoryStrategy()
    {
        allocator_ = std::make_shared<VoidAlloc>();
    }

    void add_guard_condition(const rcl_guard_condition_t *guard_condition) override
    {
        for (const auto &existing_guard_condition : guard_conditions_)
        {
            if (existing_guard_condition == guard_condition)
            {
                return;
            }
        }
        guard_conditions_.push_back(guard_condition);
    }

    void remove_guard_condition(const rcl_guard_condition_t *guard_condition) override
    {
        for (auto it = guard_conditions_.begin(); it != guard_conditions_.end(); ++it)
        {
            if (*it == guard_condition)
            {
                guard_conditions_.erase(it);
                break;
            }
        }
    }

    void clear_handles() override
    {
        subscription_handles_.clear();
        service_handles_.clear();
        client_handles_.clear();
        timer_handles_.clear();
        waitable_handles_.clear();

        // priority_queue doesn't have a clear function, so we swap it with an empty one. `empty` will go out of scope, and be cleared
        // all_executables_ = std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator>();
        std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator> empty;
        std::swap(all_executables_, empty);
        if (!all_executables_.empty())
        {
            std::cout << "couldn't clear all exec" << std::endl;
        }
    }

    void remove_null_handles(rcl_wait_set_t *wait_set) override
    {
        // TODO(jacobperron): Check if wait set sizes are what we expect them to be?
        //                    e.g. wait_set->size_of_clients == client_handles_.size()

        // Important to use subscription_handles_.size() instead of wait set's size since
        // there may be more subscriptions in the wait set due to Waitables added to the end.
        // The same logic applies for other entities.
         timespec current_time_test;
        for (size_t i = 0; i < subscription_handles_.size(); ++i)
        {
            if (!wait_set->subscriptions[i])
            {
                priority_map[subscription_handles_[i]].dont_run();
                subscription_handles_[i].reset();
            }
            else
            {
                priority_map[subscription_handles_[i]].allow_run();
                PriorityExecutable *t = get_priority_settings(subscription_handles_[i]);
                if (t->runtime_priority != nullptr && *(t->priority_dag) == -1) {
                    //if (t->runtime_priority->size() == 0) std::cout << "runtime_queue_zero: " << t->callback_id << std::endl;
                    clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                    uint64_t millis1 = current_time_test.tv_nsec; 
                    *(t->priority_dag) = t->runtime_priority->front();
                    t->runtime_priority->pop();
                    clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                    uint64_t millis2 = current_time_test.tv_nsec; 
                    std::cout << "callback_id: " << t->callback_id << " ns: " << millis2 - millis1 << std::endl;
                }               
            }
        }
        for (size_t i = 0; i < service_handles_.size(); ++i)
        {
            if (!wait_set->services[i])
            {
                priority_map[service_handles_[i]].dont_run();
                service_handles_[i].reset();
            }
            else
            {
                priority_map[service_handles_[i]].allow_run();   
            }
        }
        for (size_t i = 0; i < client_handles_.size(); ++i)
        {
            if (!wait_set->clients[i])
            {
                priority_map[client_handles_[i]].dont_run();
                client_handles_[i].reset();
            }
            else
            {
                priority_map[client_handles_[i]].allow_run();
            }
        }
        for (size_t i = 0; i < timer_handles_.size(); ++i)
        {
            if (!wait_set->timers[i])
            {
                priority_map[timer_handles_[i]].dont_run();
                timer_handles_[i].reset();
            }
            else
            {
                priority_map[timer_handles_[i]].allow_run(); 
            }
        }
        for (size_t i = 0; i < waitable_handles_.size(); ++i)
        {
            if (!waitable_handles_[i]->is_ready(wait_set))
            {
                priority_map[waitable_handles_[i]].dont_run();
                waitable_handles_[i].reset();
            }
            else
            {
                priority_map[waitable_handles_[i]].allow_run();
            }
        }

        subscription_handles_.erase(
            std::remove(subscription_handles_.begin(), subscription_handles_.end(), nullptr),
            subscription_handles_.end());

        service_handles_.erase(
            std::remove(service_handles_.begin(), service_handles_.end(), nullptr),
            service_handles_.end());

        client_handles_.erase(
            std::remove(client_handles_.begin(), client_handles_.end(), nullptr),
            client_handles_.end());

        timer_handles_.erase(
            std::remove(timer_handles_.begin(), timer_handles_.end(), nullptr),
            timer_handles_.end());

        waitable_handles_.erase(
            std::remove(waitable_handles_.begin(), waitable_handles_.end(), nullptr),
            waitable_handles_.end());
        std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator> temp;
        const PriorityExecutable* next_exec = nullptr;

        while(!all_executables_.empty()) {
            next_exec = all_executables_.top();
            all_executables_.pop();
            if (next_exec->can_be_run) {
                temp.push(next_exec);
            }
        }
        all_executables_ = temp;
    }

    bool collect_entities(const WeakNodeList &weak_nodes) override
    {
        bool has_invalid_weak_nodes = false;
        for (auto &weak_node : weak_nodes)
        {
            auto node = weak_node.lock();
            if (!node)
            {
                has_invalid_weak_nodes = true;
                continue;
            }
            for (auto &weak_group : node->get_callback_groups())
            {
                auto group = weak_group.lock();
                if (!group || !group->can_be_taken_from().load())
                {
                    continue;
                }
                group->find_subscription_ptrs_if(
                    [this](const rclcpp::SubscriptionBase::SharedPtr &subscription)
                    {
                        auto subscription_handle = subscription->get_subscription_handle();
                        PriorityExecutable *t = get_priority_settings(subscription_handle);
                        if(t == nullptr) return false;

                        subscription_handles_.push_back(subscription_handle);
                        all_executables_.push(get_and_reset_priority(subscription_handle, SUBSCRIPTION));
  
                        return false;
                    });
                group->find_service_ptrs_if(
                    [this](const rclcpp::ServiceBase::SharedPtr &service)
                    {
                        auto service_handle = service->get_service_handle();
                        PriorityExecutable *t = get_priority_settings(service_handle);
                        if(t == nullptr) return false;


                        all_executables_.push(get_and_reset_priority(service->get_service_handle(), SERVICE));
                        service_handles_.push_back(service->get_service_handle());
                        return false;
                    });
                group->find_client_ptrs_if(
                    [this](const rclcpp::ClientBase::SharedPtr &client)
                    {
                        auto client_handle = client->get_client_handle();
                        PriorityExecutable *t = get_priority_settings(client_handle);
                        if(t == nullptr) return false;

                        all_executables_.push(get_and_reset_priority(client->get_client_handle(), CLIENT));
                        client_handles_.push_back(client->get_client_handle());
                        return false;
                    });
                group->find_timer_ptrs_if(
                    [this](const rclcpp::TimerBase::SharedPtr &timer)
                    {

                        auto timer_handle = timer->get_timer_handle();
                        PriorityExecutable *t = get_priority_settings(timer_handle);
                        if(t == nullptr) return false;


                        all_executables_.push(get_and_reset_priority(timer->get_timer_handle(), TIMER));
                        timer_handles_.push_back(timer->get_timer_handle());
                        return false;
                    });
                group->find_waitable_ptrs_if(
                    [this](const rclcpp::Waitable::SharedPtr &waitable)
                    {
                        all_executables_.push(get_and_reset_priority(waitable, WAITABLE));
                        waitable_handles_.push_back(waitable);
                        return false;
                    });
            }
        }
        return has_invalid_weak_nodes;
    }

    void add_waitable_handle(const rclcpp::Waitable::SharedPtr &waitable) override
    {
        if (nullptr == waitable)
        {
            throw std::runtime_error("waitable object unexpectedly nullptr");
        }
        waitable_handles_.push_back(waitable);
    }

    bool add_handles_to_wait_set(rcl_wait_set_t *wait_set) override
    {
        for (auto subscription : subscription_handles_)
        {
            if (rcl_wait_set_add_subscription(wait_set, subscription.get(), NULL) != RCL_RET_OK)
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add subscription to wait set: %s", rcl_get_error_string().str);
                return false;
            }
        }

        for (auto client : client_handles_)
        {
            if (rcl_wait_set_add_client(wait_set, client.get(), NULL) != RCL_RET_OK)
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add client to wait set: %s", rcl_get_error_string().str);
                return false;
            }
        }

        for (auto service : service_handles_)
        {
            if (rcl_wait_set_add_service(wait_set, service.get(), NULL) != RCL_RET_OK)
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add service to wait set: %s", rcl_get_error_string().str);
                return false;
            }
        }

        for (auto timer : timer_handles_)
        {
            if (rcl_wait_set_add_timer(wait_set, timer.get(), NULL) != RCL_RET_OK)
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add timer to wait set: %s", rcl_get_error_string().str);
                return false;
            }
        }

        for (auto guard_condition : guard_conditions_)
        {
            if (rcl_wait_set_add_guard_condition(wait_set, guard_condition, NULL) != RCL_RET_OK)
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add guard_condition to wait set: %s",
                    rcl_get_error_string().str);
                return false;
            }
        }

        for (auto waitable : waitable_handles_)
        {
            if (!waitable->add_to_wait_set(wait_set))
            {
                RCUTILS_LOG_ERROR_NAMED(
                    "rclcpp",
                    "Couldn't add waitable to wait set: %s", rcl_get_error_string().str);
                return false;
            }
        }
        return true;
    }

    void
    get_next_executable(
        rclcpp::AnyExecutable &any_exec,
        const WeakNodeList &weak_nodes)
    {
        timespec current_time_test;
        const PriorityExecutable *next_exec = nullptr;
        
        //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
        //uint64_t millis1 = (current_time_test.tv_sec * (uint64_t)1000) + (current_time_test.tv_nsec / 1000000);
        //print_all_executables_();
        //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
        //uint64_t millis2 = (current_time_test.tv_sec * (uint64_t)1000) + (current_time_test.tv_nsec / 1000000);
        //std::cout << "print_spend_time: " << millis2 - millis1 << std::endl;
        
        while (!all_executables_.empty())
        {   

            next_exec = all_executables_.top();

            all_executables_.pop();

            
            if (!next_exec->can_be_run)
            {
                //std::cout << "!next_exec->can_be_run" << std::endl;
                continue;
            }

            if (next_exec->successor_callback != nullptr) {
                //std::cout << "824: " << next_exec->callback_id << " ; " << next_exec->successor_callback->callback_id << std::endl;
                //if (next_exec->successor_callback->runtime_priority == nullptr) std::cout << "nullptr runtime: " << next_exec->successor_callback->callback_id << std::endl;
                //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
            // uint64_t millis1 = (current_time_test.tv_sec * (uint64_t)1000) + (current_time_test.tv_nsec / 1000000);
                // uint64_t millis1 = current_time_test.tv_nsec;
                 next_exec->successor_callback->runtime_priority->push(*next_exec->priority_dag + 1);
                //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                // uint64_t millis2 = current_time_test.tv_nsec;
                // std::cout << "callback_id: " << next_exec->callback_id << " ns: " << millis2 - millis1 << std::endl;
            }
            // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
            // uint64_t millis2 = current_time_test.tv_nsec;
            // std::cout << "callback_id: " << next_exec->callback_id << " ns: " << millis2 - millis1 << std::endl;
            if (next_exec->successor_callbacks != nullptr) {
                // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                // uint64_t millis1 = current_time_test.tv_nsec;
                std::vector<std::pair<int, PriorityExecutable*>> vec = (*next_exec->successor_callbacks)[*next_exec->priority_dag];
                // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                // uint64_t millis2 = current_time_test.tv_nsec;
                // std::cout << "callback_id: " << next_exec->callback_id << " ns: " << millis2 - millis1 << std::endl;
                for (size_t i = 0; i < vec.size(); ++i) {
                    // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                    // uint64_t millis1 = current_time_test.tv_nsec;
                    vec[i].second->runtime_priority->push(vec[i].first);
                    // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
                    // uint64_t millis2 = current_time_test.tv_nsec;
                    // std::cout << "callback_id: " << next_exec->callback_id << " ns: " << millis2 - millis1 << std::endl;                    
                }
            }
            if (next_exec->runtime_priority != nullptr) {
                // if (next_exec->runtime_priority->size() == 0) std::cout << "runtime_queue is zero: " << next_exec->callback_id << std::endl;
                // next_exec->runtime_priority->pop();
                *next_exec->priority_dag = -1;
            }
            // clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
            // uint64_t millis2 = (current_time_test.tv_sec * (uint64_t)1000) + (current_time_test.tv_nsec / 1000000);
            // std::cout << " time: " << millis2 - millis1 << std::endl;
            ExecutableType type = next_exec->type;
            switch (type)
            {
            case SUBSCRIPTION:
            {
                std::shared_ptr<const rcl_subscription_t> subs_handle = std::static_pointer_cast<const rcl_subscription_t>(next_exec->handle);
                auto subscription = get_subscription_by_handle(subs_handle, weak_nodes);
                if (subscription)
                {
                    auto group = get_group_by_subscription(subscription, weak_nodes);
                    if (!group)
                    {
                        // Group was not found, meaning the waitable is not valid...
                        // Remove it from the ready list and continue looking
                        //it = subscription_handles_.erase(it);
                        continue;
                    }
                    if (!group->can_be_taken_from().load())
                    {
                        // Group is mutually exclusive and is being used, so skip it for now
                        // Leave it to be checked next time, but continue searching
                        //++it;
                        continue;
                    }
                    any_exec.callback_group = group;
                    any_exec.subscription = subscription;
                    any_exec.node_base = get_node_by_group(group, weak_nodes);
                    // std::cout << "Using new priority sub " << subscription->get_topic_name() << std::endl;
                }
            }
            break;
            case SERVICE:
            {
                std::shared_ptr<const rcl_service_t> service_handle = std::static_pointer_cast<const rcl_service_t>(next_exec->handle);
                auto service = get_service_by_handle(service_handle, weak_nodes);
                if (service)
                {
                    auto group = get_group_by_service(service, weak_nodes);
                    if (!group)
                    {
                        // Group was not found, meaning the waitable is not valid...
                        // Remove it from the ready list and continue looking
                        // it = subscription_handles_.erase(it);
                        continue;
                    }
                    if (!group->can_be_taken_from().load())
                    {
                        // Group is mutually exclusive and is being used, so skip it for now
                        // Leave it to be checked next time, but continue searching
                        // ++it;
                        continue;
                    }
                    any_exec.callback_group = group;
                    any_exec.service = service;
                    any_exec.node_base = get_node_by_group(group, weak_nodes);
                    // std::cout << "Using new priority service " << service->get_service_name() << std::endl;
                }
            }
            break;
            case CLIENT:
            {
                std::shared_ptr<const rcl_client_t> client_handle = std::static_pointer_cast<const rcl_client_t>(next_exec->handle);
                auto client = get_client_by_handle(client_handle, weak_nodes);
                if (client)
                {
                    auto group = get_group_by_client(client, weak_nodes);
                    if (!group)
                    {
                        // Group was not found, meaning the waitable is not valid...
                        // Remove it from the ready list and continue looking
                        // it = subscription_handles_.erase(it);
                        continue;
                    }
                    if (!group->can_be_taken_from().load())
                    {
                        // Group is mutually exclusive and is being used, so skip it for now
                        // Leave it to be checked next time, but continue searching
                        // ++it;
                        continue;
                    }
                    any_exec.callback_group = group;
                    any_exec.client = client;
                    any_exec.node_base = get_node_by_group(group, weak_nodes);
                    // std::cout << "Using new priority client " << client->get_service_name() << std::endl;
                }
            }
            break;
            case TIMER:
            {
                std::shared_ptr<const rcl_timer_t> timer_handle = std::static_pointer_cast<const rcl_timer_t>(next_exec->handle);
                auto timer = get_timer_by_handle(timer_handle, weak_nodes);
                if (timer)
                {
                    auto group = get_group_by_timer(timer, weak_nodes);
                    if (!group)
                    {
                        // Group was not found, meaning the waitable is not valid...
                        // Remove it from the ready list and continue looking
                        //it = subscription_handles_.erase(it);
                        continue;
                    }
                    if (!group->can_be_taken_from().load())
                    {
                        // Group is mutually exclusive and is being used, so skip it for now
                        // Leave it to be checked next time, but continue searching
                        //++it;
                        continue;
                    }
                    any_exec.callback_group = group;
                    any_exec.timer = timer;
                    any_exec.node_base = get_node_by_group(group, weak_nodes);
                }
            }
            break;
            case WAITABLE:
            {
                std::shared_ptr<rclcpp::Waitable> waitable_handle = std::static_pointer_cast<rclcpp::Waitable>(next_exec->waitable);
                auto waitable = waitable_handle;
                if (waitable)
                {
                    auto group = get_group_by_waitable(waitable, weak_nodes);
                    if (!group)
                    {
                        // Group was not found, meaning the waitable is not valid...
                        // Remove it from the ready list and continue looking
                        // it = subscription_handles_.erase(it);
                        continue;
                    }
                    if (!group->can_be_taken_from().load())
                    {
                        // Group is mutually exclusive and is being used, so skip it for now
                        // Leave it to be checked next time, but continue searching
                        // ++it;
                        continue;
                    }
                    any_exec.callback_group = group;
                    any_exec.waitable = waitable;
                    any_exec.node_base = get_node_by_group(group, weak_nodes);
                    // std::cout << "Using new priority waitable" << std::endl;
                }
            }
            break;
            default:
                // std::cout << "Unknown type from priority!!!" << std::endl;
                break;
            }
            // callback is about to be released
            *(next_exec->sum) += 1;
            if (next_exec->is_first_in_chain && next_exec->sched_type != DEADLINE)
            {
                //timespec current_time;
                //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
                //uint64_t millis = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);

                //auto timer = next_exec->timer_handle;
                //int64_t time_until_next_call = timer->time_until_trigger().count() / 1000000;
                //std::cout << "end of chain. time until trigger: " << std::to_string(time_until_next_call) << std::endl;
                // log_entry(logger, "timer_" + std::to_string(next_exec->chain_id) + "_release_" + std::to_string(millis + time_until_next_call));
                if (next_exec->chain_id == 0 && is_f1tenth)
                {
                    // special case for logging the shared timer
                    //log_entry(logger, "timer_" + std::to_string(next_exec->chain_id + 1) + "_release_" + std::to_string(millis + time_until_next_call));
                }
            }
            //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time_test);
            //uint64_t millis2 = (current_time_test.tv_sec * (uint64_t)1000) + (current_time_test.tv_nsec / 1000000);
            //std::cout << "current_time_test: " << millis2 - millis1 << " current_time: " << millis2 << std::endl;
            //std::cout << "is_first_in_chain: " << next_exec->is_first_in_chain << " sched_type: " << next_exec->sched_type << std::endl;
            if (next_exec->is_first_in_chain && next_exec->sched_type == DEADLINE)
            {
                if (next_exec->timer_handle == nullptr)
                {
                    std::cout << "tried to use a chain without a timer handle!!!" << std::endl;
                }
                timespec current_time;
                clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
                uint64_t millis = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);
                //std::cout << "current_time: " << millis << " chain_id: " << next_exec->chain_id << std::endl;
                auto timer = next_exec->timer_handle;
                //std::cout << "timer->time_until_trigger().count(): " << timer->time_until_trigger().count() / 1000000 << std::endl;
                //std::cout << "in executor get release time: " << millis + timer->time_until_trigger().count() / 1000000 << std::endl;
                //std::cout << "record release time: " << *next_exec->release_time << " chain_index: " << next_exec->chain_id << std::endl;
                //log_entry(logger, std::to_string(next_exec->chain_id) + " release_time: " + std::to_string(*next_exec->release_time));    
                int64_t time_diff = millis - *next_exec->release_time;
                if (time_diff < 0) time_diff = -time_diff;
                int max_chain_num = std::ceil(next_exec->deadline / (double)next_exec->period);
                int index = ((*next_exec->cur_index) + 1) % max_chain_num;
                //std::cout << "max_chain_num: " << max_chain_num << " cur_index: " << (*next_exec->cur_index) << " index: " << index << std::endl;
                if (time_diff < next_exec->period) {
                    //std::cout << "before add: " << *next_exec->release_time << std::endl; 
                    *next_exec->release_time = *next_exec->release_time + next_exec->period;
                    //std::cout << "after add: " << *next_exec->release_time << " " << next_exec->period << std::endl;
                    (*next_exec->deadlines)[index]->push_back(*next_exec->release_time + next_exec->deadline);
                    //next_exec->deadlines->push_back(*next_exec->release_time + next_exec->deadline);
                    if (is_f1tenth && next_exec->chain_id == 0) {
                        for (auto it : priority_map) {
                            if(it.second.chain_id == 1) {
                                (*it.second.deadlines)[index]->push_back(*next_exec->release_time + next_exec->deadline);
                                //it.second.deadlines->push_back(*next_exec->release_time + next_exec->deadline);
                                break;
                            }
                        }
                    }
                } else {
                    //std::cout << "before add: " << *next_exec->release_time << std::endl; 
                    int periods_late = std::ceil(time_diff / (double)next_exec->period);
                    //std::cout << "periods_late: " << periods_late << " millis: " << millis << " release_time: " << *next_exec->release_time << std::endl; 
                    *next_exec->release_time = *next_exec->release_time + (periods_late) * next_exec->period;
                    //std::cout << "after add: " << *next_exec->release_time << " " << next_exec->period << " " << periods_late << std::endl;
                    (*next_exec->deadlines)[index]->push_back(*next_exec->release_time + next_exec->deadline);
                    //next_exec->deadlines->push_back(*next_exec->release_time + next_exec->deadline);
                    if (is_f1tenth && next_exec->chain_id == 0) {
                        for (auto it : priority_map) {
                            if(it.second.chain_id == 1) {
                                (*it.second.deadlines)[index]->push_back(*next_exec->release_time + next_exec->deadline);
                                //it.second.deadlines->push_back(*next_exec->release_time + next_exec->deadline);
                                break;
                            }
                        }
                    }
                }
            }
            if (next_exec->is_last_in_chain && next_exec->sched_type == DEADLINE)
            {
                /*
                if(next_exec->chain_id == 0 || next_exec->chain_id == 1) {
                    for(auto it : priority_map) {
                        if(it.second.chain_id == 0 && it.second.is_first_in_chain) {
                            std::cout << "size: " << it.second.deadlines->size() << std::endl;
                        }
                    }
                }
                */
               /*
                timespec current_time;
                clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
                uint64_t millis = (current_time.tv_sec * 1000UL) + (current_time.tv_nsec / 1000000);

                uint64_t this_deadline = next_exec->deadlines->front();
                
                std::ostringstream oss;
                if (next_exec->timer_handle == nullptr)
                {
                    std::cout << "tried to use a chain without a timer handle!!!" << std::endl;
                }
                auto timer = next_exec->timer_handle;
                std::cout << "chain_id: " << next_exec->chain_id <<" last_chain_until_next_timer: " << millis + timer->time_until_trigger().count() / 1000000 << std::endl;
                std::cout << "this_deadline: " << this_deadline << std::endl;
                if (timer == nullptr)
                {
                    std::cout << "somehow, this timer handle didn't have an associated timer" << std::endl;
                }
                uint64_t next_deadline = 0;
                bool on_time;
                int64_t time_diff = millis - this_deadline;
                //std::cout << " id: " << next_exec->chain_id <<" millis: " << millis << " this_deadline: " << this_deadline << std::endl;
                int periods_late;
                if (time_diff < 0)
                {
                    periods_late = 0;
                    next_deadline = this_deadline + next_exec->period;
                    //std::cout << "id: " << next_exec->chain_id << " this_deadline: " << this_deadline << " period: " << next_exec->period << std::endl;
                    on_time = true;
                }
                // if time_diff is positive, we completed late. add one period for each period we were late
                else
                {
                    periods_late = std::ceil(time_diff / (double)next_exec->period);
                    next_deadline = this_deadline + (periods_late + 1) * next_exec->period;
                    on_time = false;
                    //std::cout << "on_time_false, chain_id: " << next_exec->chain_id << " time_diff: " << time_diff << std::endl;
                    //std::cout << "deadlines size: " << next_exec->deadlines->size() << std::endl;
                }
                 oss << "{\"operation\": \"next_deadline\", \"chain_id\": " << next_exec->chain_id << ", \"deadline\": " << next_deadline << ", \"on_time\": " << on_time << ", \"time_diff\": " << time_diff << ", \"periods_late\": " << periods_late;
                log_entry(logger, oss.str());
                next_exec->deadlines->push_back(next_deadline);
                */
                
               
                /*if(is_f1tenth && (next_exec->chain_id == 0 || next_exec->chain_id == 1)) {
                    for(auto it : priority_map) {
                        if(it.second.chain_id == 0 && it.second.is_first_in_chain) {
                            if(it.second.deadlines->front() > next_deadline || it.second.deadlines->empty()) {
                                if(!it.second.deadlines->empty()) {
                                   // std::cout << "front data: " << it.second.deadlines->front() << " deadlines size: " << it.second.deadlines->size() << std::endl;
                                    it.second.deadlines->pop_front();
                                }
                                //std::cout << "next_deadline: " << next_deadline << std::endl;
                                it.second.deadlines->push_back(next_deadline);
                                //std::cout << "deadlines size: " << it.second.deadlines->size() << std::endl;
                            }
                            break;
                        }
                    }
                }*/
                int max_chain_num = std::ceil(next_exec->deadline / (double)next_exec->period);
                //int index = ((*next_exec->cur_index) + 1) % max_chain_num;
                if (!(*next_exec->deadlines)[(*next_exec->cur_index)]->empty())
                    (*next_exec->deadlines)[(*next_exec->cur_index)]->pop_front();
                    //next_exec->deadlines->pop_front();
            }
            if (next_exec->sched_type == DEADLINE) {
                int max_chain_num = std::ceil(next_exec->deadline / (double)next_exec->period);
                (*next_exec->cur_index) = ((*next_exec->cur_index) + 1) % max_chain_num;        
            }
            if (next_exec->sched_type == CHAIN_AWARE_PRIORITY || next_exec->sched_type == DEADLINE)
            {
                // this is safe, since we popped it earlier
                // get a mutable reference
                // TODO: find a cleaner way to do this
                PriorityExecutable *mut_executable = get_priority_settings(next_exec->handle);
                // std::cout << "running chain aware cb" << std::endl;
                mut_executable->increment_counter();
            }
            if (next_exec->sched_type == CHAIN_AWARE_PRIORITY && next_exec->is_first_in_chain) {
                //std::cout << "chain_id: " << next_exec->chain_id << std::endl;
                //timespec current_time;
                //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
                //uint64_t millis = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);
                //auto timer = next_exec->timer_handle;
                //int64_t time_until_next_call = timer->time_until_trigger().count() / 1000000;
                //int64_t release_time = millis + time_until_next_call;
                //log_entry(logger, std::to_string(next_exec->chain_id) + " release_time: " + std::to_string(release_time)); 
            }
            return;
        }
    }

    void
    get_next_subscription(
        rclcpp::AnyExecutable &any_exec,
        const WeakNodeList &weak_nodes) override
    {
        auto it = subscription_handles_.begin();
        while (it != subscription_handles_.end())
        {
            auto subscription = get_subscription_by_handle(*it, weak_nodes);
            if (subscription)
            {
                // Find the group for this handle and see if it can be serviced
                auto group = get_group_by_subscription(subscription, weak_nodes);
                if (!group)
                {
                    // Group was not found, meaning the subscription is not valid...
                    // Remove it from the ready list and continue looking
                    it = subscription_handles_.erase(it);
                    continue;
                }
                if (!group->can_be_taken_from().load())
                {
                    // Group is mutually exclusive and is being used, so skip it for now
                    // Leave it to be checked next time, but continue searching
                    ++it;
                    continue;
                }
                // Otherwise it is safe to set and return the any_exec
                any_exec.subscription = subscription;
                any_exec.callback_group = group;
                any_exec.node_base = get_node_by_group(group, weak_nodes);
                subscription_handles_.erase(it);
                return;
            }
            // Else, the subscription is no longer valid, remove it and continue
            it = subscription_handles_.erase(it);
        }
    }

    void
    get_next_service(
        rclcpp::AnyExecutable &any_exec,
        const WeakNodeList &weak_nodes) override
    {
        auto it = service_handles_.begin();
        while (it != service_handles_.end())
        {
            auto service = get_service_by_handle(*it, weak_nodes);
            if (service)
            {
                // Find the group for this handle and see if it can be serviced
                auto group = get_group_by_service(service, weak_nodes);
                if (!group)
                {
                    // Group was not found, meaning the service is not valid...
                    // Remove it from the ready list and continue looking
                    it = service_handles_.erase(it);
                    continue;
                }
                if (!group->can_be_taken_from().load())
                {
                    // Group is mutually exclusive and is being used, so skip it for now
                    // Leave it to be checked next time, but continue searching
                    ++it;
                    continue;
                }
                // Otherwise it is safe to set and return the any_exec
                any_exec.service = service;
                any_exec.callback_group = group;
                any_exec.node_base = get_node_by_group(group, weak_nodes);
                service_handles_.erase(it);
                return;
            }
            // Else, the service is no longer valid, remove it and continue
            it = service_handles_.erase(it);
        }
    }

    void
    get_next_client(rclcpp::AnyExecutable &any_exec, const WeakNodeList &weak_nodes) override
    {
        auto it = client_handles_.begin();
        while (it != client_handles_.end())
        {
            auto client = get_client_by_handle(*it, weak_nodes);
            if (client)
            {
                // Find the group for this handle and see if it can be serviced
                auto group = get_group_by_client(client, weak_nodes);
                if (!group)
                {
                    // Group was not found, meaning the service is not valid...
                    // Remove it from the ready list and continue looking
                    it = client_handles_.erase(it);
                    continue;
                }
                if (!group->can_be_taken_from().load())
                {
                    // Group is mutually exclusive and is being used, so skip it for now
                    // Leave it to be checked next time, but continue searching
                    ++it;
                    continue;
                }
                // Otherwise it is safe to set and return the any_exec
                any_exec.client = client;
                any_exec.callback_group = group;
                any_exec.node_base = get_node_by_group(group, weak_nodes);
                client_handles_.erase(it);
                return;
            }
            // Else, the service is no longer valid, remove it and continue
            it = client_handles_.erase(it);
        }
    }

    void
    get_next_timer(
        rclcpp::AnyExecutable &any_exec,
        const WeakNodeList &weak_nodes) override
    {
        auto it = timer_handles_.begin();
        while (it != timer_handles_.end())
        {
            auto timer = get_timer_by_handle(*it, weak_nodes);
            if (timer)
            {
                // Find the group for this handle and see if it can be serviced
                auto group = get_group_by_timer(timer, weak_nodes);
                if (!group)
                {
                    // Group was not found, meaning the timer is not valid...
                    // Remove it from the ready list and continue looking
                    it = timer_handles_.erase(it);
                    continue;
                }
                if (!group->can_be_taken_from().load())
                {
                    // Group is mutually exclusive and is being used, so skip it for now
                    // Leave it to be checked next time, but continue searching
                    ++it;
                    continue;
                }
                // Otherwise it is safe to set and return the any_exec
                any_exec.timer = timer;
                any_exec.callback_group = group;
                any_exec.node_base = get_node_by_group(group, weak_nodes);
                timer_handles_.erase(it);
                return;
            }
            // Else, the service is no longer valid, remove it and continue
            it = timer_handles_.erase(it);
        }
    }

    void
    get_next_waitable(rclcpp::AnyExecutable &any_exec, const WeakNodeList &weak_nodes) override
    {
        auto it = waitable_handles_.begin();
        while (it != waitable_handles_.end())
        {
            auto waitable = *it;
            if (waitable)
            {
                // Find the group for this handle and see if it can be serviced
                auto group = get_group_by_waitable(waitable, weak_nodes);
                if (!group)
                {
                    // Group was not found, meaning the waitable is not valid...
                    // Remove it from the ready list and continue looking
                    it = waitable_handles_.erase(it);
                    continue;
                }
                if (!group->can_be_taken_from().load())
                {
                    // Group is mutually exclusive and is being used, so skip it for now
                    // Leave it to be checked next time, but continue searching
                    ++it;
                    continue;
                }
                // Otherwise it is safe to set and return the any_exec
                any_exec.waitable = waitable;
                any_exec.callback_group = group;
                any_exec.node_base = get_node_by_group(group, weak_nodes);
                waitable_handles_.erase(it);
                return;
            }
            // Else, the waitable is no longer valid, remove it and continue
            it = waitable_handles_.erase(it);
        }
    }

    rcl_allocator_t get_allocator() override
    {
        return rclcpp::allocator::get_rcl_allocator<void *, VoidAlloc>(*allocator_.get());
    }

    size_t number_of_ready_subscriptions() const override
    {
        size_t number_of_subscriptions = subscription_handles_.size();
        // std::cout << "ready_raw: " << number_of_subscriptions << std::endl;
        for (auto waitable : waitable_handles_)
        {
            number_of_subscriptions += waitable->get_number_of_ready_subscriptions();
        }
        return number_of_subscriptions;
    }

    size_t number_of_ready_services() const override
    {
        size_t number_of_services = service_handles_.size();
        for (auto waitable : waitable_handles_)
        {
            number_of_services += waitable->get_number_of_ready_services();
        }
        return number_of_services;
    }

    size_t number_of_ready_events() const override
    {
        size_t number_of_events = 0;
        for (auto waitable : waitable_handles_)
        {
            number_of_events += waitable->get_number_of_ready_events();
        }
        return number_of_events;
    }

    size_t number_of_ready_clients() const override
    {
        size_t number_of_clients = client_handles_.size();
        for (auto waitable : waitable_handles_)
        {
            number_of_clients += waitable->get_number_of_ready_clients();
        }
        return number_of_clients;
    }

    size_t number_of_guard_conditions() const override
    {
        size_t number_of_guard_conditions = guard_conditions_.size();
        for (auto waitable : waitable_handles_)
        {
            number_of_guard_conditions += waitable->get_number_of_ready_guard_conditions();
        }
        return number_of_guard_conditions;
    }

    size_t number_of_ready_timers() const override
    {
        size_t number_of_timers = timer_handles_.size();
        for (auto waitable : waitable_handles_)
        {
            number_of_timers += waitable->get_number_of_ready_timers();
        }
        return number_of_timers;
    }

    size_t number_of_waitables() const override
    {
        return waitable_handles_.size();
    }

    void set_executable_priority(std::shared_ptr<const void> handle, int priority, ExecutableType t)
    {
        // TODO: any sanity checks should go here
        // priority_map.insert(executable, priority);
        priority_map[handle] = PriorityExecutable(handle, priority, t);
    }
    void set_executable_priority(std::shared_ptr<const void> handle, int priority, ExecutableType t, ExecutableScheduleType sc, int chain_index)
    {
        // TODO: any sanity checks should go here
        // priority_map.insert(executable, priority);
        priority_map[handle] = PriorityExecutable(handle, priority, t, sc);
        priority_map[handle].chain_id = chain_index;
    }
    void set_executable_deadline(std::shared_ptr<const void> handle, int period, int deadline, ExecutableType t, int chain_id = 0)
    {
        // TODO: any sanity checks should go here
        // priority_map.insert(executable, priority);
        priority_map[handle] = PriorityExecutable(handle, period, deadline, t, DEADLINE);
        priority_map[handle].chain_id = chain_id;
    }

    void set_executable_priority_dag(std::shared_ptr<const void> handle, int priority, ExecutableType t, ExecutableScheduleType sc, int callback_id, std::unordered_map<int, std::vector<std::pair<int, PriorityExecutable*>>>* successor_callbacks) 
    {
        priority_map[handle] = PriorityExecutable(handle, priority, t, successor_callbacks, sc);
        priority_map[handle].callback_id = callback_id;
    }
    int get_priority(std::shared_ptr<const void> executable)
    {
        auto search = priority_map.find(executable);
        if (search != priority_map.end())
        {
            return search->second.priority;
        }
        else
        {
            return 0;
        }
    }

    PriorityExecutable *get_priority_settings(std::shared_ptr<const void> executable)
    {
        auto search = priority_map.find(executable);
        if (search != priority_map.end())
        {
            return &(search->second);
        }
        else
        {
            return nullptr;
        }
    }

    void set_first_in_chain(std::shared_ptr<const void> exec_handle)
    {
        PriorityExecutable *settings = get_priority_settings(exec_handle);
        settings->is_first_in_chain = true;
    }

    void set_last_in_chain(std::shared_ptr<const void> exec_handle)
    {
        PriorityExecutable *settings = get_priority_settings(exec_handle);
        settings->is_last_in_chain = true;
    }

    void assign_deadlines_queue(std::shared_ptr<const void> exec_handle, std::vector<std::deque<uint> *> *deadlines)
    {
        PriorityExecutable *settings = get_priority_settings(exec_handle);
        settings->deadlines = deadlines;
    }

    void assign_release_time(std::shared_ptr<const void> exec_handle, int release_time) {
        PriorityExecutable *settings = get_priority_settings(exec_handle);
        settings->release_time = new long(release_time);
    }

    void print_all_handle_schedule_type() {
        for(auto it : priority_map) {
            std::cout << "chain_id: " << it.second.chain_id;
            std::cout << " _schedule_type: " << it.second.sched_type;
            std::cout << " _type: " << it.second.type;
            //std::cout << " _deadline: " << (it.second.deadlines == nullptr ? -1 : it.second.deadlines->front())<< std::endl;
            if(it.second.deadlines->empty()) std::cout << " _deadlines: "<< "nullptr";
            else std::cout << " _deadlines: " << it.second.deadlines->front();
            std::cout << " count_chain: " << *(it.second.sum);
            std::cout << " is_first_in_chain: " << (it.second.is_first_in_chain ? "yes" : "no") << std::endl;
        }
    }
    void print_all_dag() {
        for (auto it : priority_map) {
            std::cout << "callback_id: " << it.second.callback_id;
            std::cout << " priority: " << *(it.second.priority_dag);
            std::cout << " type: " << it.second.type <<std::endl;
            if (it.second.successor_callback != nullptr) {
                std::cout << " successor: " << it.second.successor_callback->callback_id << std::endl;
            }
            if (it.second.runtime_priority == nullptr) {
                std::cout << "runtime_queue_not_create!" << std::endl;
            }
            // if (it.second.successor_callback != nullptr){
            //     std::cout << " successor: " << it.second.successor_callback->callback_id << std::endl;
            //     std::cout << " queue_counter: " << it.second.queue_counter << std::endl;
            // }
            // std::cout << " count: " << it.second.counter << std::endl;
            // std::cout << " release_count: " << it.second.release_counter << std::endl;
        }
    }
    void print_all_executables_() {
        timespec current_time;
        clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
        uint64_t millis = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);
        std::cout << "print_all_can_be_run_executables thread_id: " << pthread_self() << " current_time: " << millis << std::endl;
        //std::cout << " current_time: " << millis << std::endl;
        //std::cout << "size: " << all_executables_.size() << std::endl;
        std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator> temp;
        temp = all_executables_;

        const PriorityExecutable *next_exec = nullptr;
        while(!temp.empty()) {
            next_exec = temp.top();
            temp.pop();
            if(next_exec->can_be_run) {
                std::cout << "_schedule_type: " << next_exec->sched_type;
                std::cout << " chain_id: " << next_exec->chain_id;
                std::cout << " is_first_in_chain: " << next_exec->is_first_in_chain;
                //std::cout << " deadlines: " << (next_exec->deadlines == nullptr ? -1 : next_exec->deadlines->front())<< std::endl;
                if(next_exec->deadlines == nullptr) {
                    std::cout << " deadlines nullptr";
                } else {
                    std::cout << " deadlines: " << (*next_exec->deadlines)[*next_exec->cur_index]->front();
                    std::cout << " deadlines_size: " << (*next_exec->deadlines)[*next_exec->cur_index]->size();
                    std::cout << " cur_index: " << *next_exec->cur_index;
                }
                std::cout << " type: " << next_exec->type << std::endl;
                //std::cout << " count_chain: " << *next_exec->sum << std::endl;
                //std::cout << " current_thread: " << pthread_self() << std::endl;
                //if(next_exec->chain_id == 5) std::cout << "---------------" << std::endl;
            }
        }
        //clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
        //uint64_t millis1 = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);
        //std::cout << "time spend: " << millis1 - millis << std::endl;
    }
    void print_dag() {
        timespec current_time;
        clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
        uint64_t millis = (current_time.tv_sec * (uint64_t)1000) + (current_time.tv_nsec / 1000000);
        std::cout << "--------------" << std::endl;
        std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator> temp;
        temp = all_executables_;
        const PriorityExecutable *next_exec = nullptr;
        while(!temp.empty()) {
            next_exec = temp.top();
            temp.pop();
            if(next_exec->can_be_run) {
                std::cout << " callback_id: " << next_exec->callback_id << " priority: " << *next_exec->priority_dag << " can_be_run: " << next_exec->can_be_run << std::endl;
            }
        }
        std::cout << "--------------" << std::endl;
    }
private:
    PriorityExecutable *get_and_reset_priority(std::shared_ptr<const void> executable, ExecutableType t)
    {
        PriorityExecutable *p = get_priority_settings(executable);
        if (p == nullptr)
        {
            priority_map[executable] = PriorityExecutable(executable, 0, t);
            p = &(priority_map[executable]);
        }
        // p->can_be_run = true;
        return p;
    }

    template <typename T>
    using VectorRebind =
        std::vector<T, typename std::allocator_traits<Alloc>::template rebind_alloc<T>>;

    VectorRebind<const rcl_guard_condition_t *> guard_conditions_;

    VectorRebind<std::shared_ptr<const rcl_subscription_t>> subscription_handles_;
    VectorRebind<std::shared_ptr<const rcl_service_t>> service_handles_;
    VectorRebind<std::shared_ptr<const rcl_client_t>> client_handles_;
    VectorRebind<std::shared_ptr<const rcl_timer_t>> timer_handles_;
    VectorRebind<std::shared_ptr<rclcpp::Waitable>> waitable_handles_;


    std::shared_ptr<VoidAlloc> allocator_;

    // TODO: evaluate using node/subscription namespaced strings as keys

    // holds *all* handle->priority mappings
    std::unordered_map<std::shared_ptr<const void>, PriorityExecutable> priority_map;

    // hold *only valid* executable+priorities
    std::priority_queue<const PriorityExecutable *, std::vector<const PriorityExecutable *>, PriorityExecutableComparator> all_executables_;
};

#endif // RCLCPP__STRATEGIES__ALLOCATOR_MEMORY_STRATEGY_HPP_
