#pragma once

#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/policies.hpp>
#include <atomic>
#include <thread>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <optional>
#include <functional>
#include <algorithm>
#include <iostream>

namespace fast_queue {

struct ThreadExitHelper {
    std::function<void()> exitCallback;
    ThreadExitHelper(const std::function<void()> &cb) : exitCallback(cb) {}
    ~ThreadExitHelper() {
        exitCallback();
    }
};

template<typename T>
struct QueueTraits {
    using value_type = T;

    static constexpr size_t BATCH_THRESHOLD = 32;

    static constexpr size_t INITIAL_LOCAL_QUEUE_SIZE = 256;

    static constexpr bool ENABLE_STATS = false;

    using allocator = std::allocator<T>;
};

template<typename T, typename Traits = QueueTraits<T>>
class SingleConsumerQueue {
private:

    using LocalQueue = boost::lockfree::queue<
        T, 
        boost::lockfree::allocator<typename Traits::allocator>
    >;

    struct ProducerInfo {
        std::unique_ptr<LocalQueue> queue;
        std::atomic<size_t> approx_size{0};

        ProducerInfo(size_t size) : queue(std::make_unique<LocalQueue>(size)) {}

        bool is_empty() const {
            if (approx_size.load(std::memory_order_acquire) == 0) {
                return true;
            }
            if (!queue) {
                return false;
            }
            return queue->empty();
        }
    };

    struct Stats {
        std::atomic<size_t> total_enqueues{0};
        std::atomic<size_t> total_dequeues{0};
        std::atomic<size_t> queue_empty_count{0};
        std::atomic<size_t> successful_steals{0};
    };

private:
    mutable std::shared_mutex producers_mutex_;
    // producers access to find itself
    std::unordered_map<std::thread::id, std::shared_ptr<ProducerInfo>> producer_map_;
    // consumer accesses to iterate all producers
    std::vector<std::shared_ptr<ProducerInfo>> active_producers_;

    Stats stats_;

    size_t initial_queue_size_ = Traits::INITIAL_LOCAL_QUEUE_SIZE;

    std::thread::id consumer_thread_id_;
    mutable std::atomic<bool> has_consumer_{false};

public:
    SingleConsumerQueue() {}

    SingleConsumerQueue(size_t queueSize) : initial_queue_size_(queueSize) {}

    ~SingleConsumerQueue() {
        clear_all_producers();
    }

    SingleConsumerQueue(const SingleConsumerQueue&) = delete;
    SingleConsumerQueue& operator=(const SingleConsumerQueue&) = delete;
    
    void setConsumer(std::thread::id threadId) {
        consumer_thread_id_ = threadId;
        has_consumer_.store(true, std::memory_order_release);
    }

    bool enqueue(const T& value) {
        return enqueue_to_producer(value);
    }

    bool enqueue(T&& value) {
        return enqueue_to_producer(std::move(value));
    }

    template<typename InputIterator>
    bool enqueue_bulk(InputIterator first, size_t count) {
        if (count == 0) return true;

        if (count <= Traits::BATCH_THRESHOLD) {
            bool all_success = true;
            for (size_t i = 0; i < count; ++i) {
                if (!enqueue(*first++)) {
                    all_success = false;
                }
            }
            return all_success;
        }

        return enqueue_bulk_to_producer(first, count);
    }

    bool dequeue(T& value) {
        if (!is_consumer_thread()) {
            return false;
        }

        if (try_steal_from_producers(value)) {
            if constexpr (Traits::ENABLE_STATS) {
                stats_.successful_steals.fetch_add(1, std::memory_order_relaxed);
                stats_.total_dequeues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        if constexpr (Traits::ENABLE_STATS) {
            stats_.queue_empty_count.fetch_add(1, std::memory_order_relaxed);
        }
        return false;
    }

    size_t dequeue_bulk(std::function<void(T)> &&func, size_t max_count) {
        if (!is_consumer_thread() || max_count == 0) {
            return 0;
        }
        
        size_t total_dequeued = steal_bulk_from_producers(std::forward<std::function<void(T)>>(func), max_count);

        return total_dequeued;
    }

    bool try_dequeue(T& value) {
        return dequeue(value);
    }

    size_t size_approx() const {
        size_t total_size = 0;
        {
            std::shared_lock lock(producers_mutex_);
            for (auto producer : active_producers_) {
                if (producer) {
                    total_size += producer->approx_size.load(std::memory_order_relaxed);
                }
            }
        }

        return total_size;
    }

    bool empty() const {
        std::shared_lock lock(producers_mutex_);
        for (auto producer : active_producers_) {
            if (producer && !producer->is_empty()) {
                return false;
            }
        }

        return true;
    }

    void clear() {
        clear_all_producers();
    }

    template<bool Enabled = Traits::ENABLE_STATS>
    std::enable_if_t<Enabled, Stats> get_stats() const {
        return stats_;
    }

    size_t active_producer_count() const {
        std::shared_lock lock(producers_mutex_);
        return active_producers_.size();
    }

private:
    bool is_consumer_thread() const {
        return has_consumer_.load(std::memory_order_acquire) && 
               std::this_thread::get_id() == consumer_thread_id_;
    }

    bool enqueue_to_producer(const T& value) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        if (producer_info->queue->push(value)) {
            producer_info->approx_size.fetch_add(1, std::memory_order_relaxed);

            if constexpr (Traits::ENABLE_STATS) {
                stats_.total_enqueues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        return false;
    }

    bool enqueue_to_producer(T&& value) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        if (producer_info->queue->push(std::move(value))) {
            producer_info->approx_size.fetch_add(1, std::memory_order_relaxed);

            if constexpr (Traits::ENABLE_STATS) {
                stats_.total_enqueues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        return false;
    }

    template<typename InputIterator>
    bool enqueue_bulk_to_producer(InputIterator first, size_t count) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        size_t success_count = 0;
        for (size_t i = 0; i < count; ++i) {
            if (producer_info->queue->push(*first++)) {
                success_count++;
            } else {
                break;
            }
        }

        producer_info->approx_size.fetch_add(success_count, std::memory_order_relaxed);

        if constexpr (Traits::ENABLE_STATS) {
            stats_.total_enqueues.fetch_add(success_count, std::memory_order_relaxed);
        }

        return success_count == count;
    }

    bool try_steal_from_producers(T& value) {
        std::shared_lock lock(producers_mutex_);

        if (active_producers_.empty()) {
            return false;
        }

        static thread_local size_t steal_index = 0;
        size_t start_index = steal_index++ % active_producers_.size();

        for (size_t i = 0; i < active_producers_.size(); ++i) {
            size_t idx = (start_index + i) % active_producers_.size();
            auto producer = active_producers_[idx];

            if (producer) {
                if (producer->queue->pop(value)) {
                    producer->approx_size.fetch_sub(1, std::memory_order_relaxed);
                    return true;
                }
            }
        }

        return false;
    }

    size_t steal_bulk_from_producers(std::function<void(T)> &&func, size_t max_count) {
        if (max_count == 0) return 0;

        std::shared_lock lock(producers_mutex_);
        size_t total_stolen = 0;

        static thread_local size_t steal_index = 0;
        size_t start_index = steal_index++ % active_producers_.size();

        for (size_t i = 0; i < active_producers_.size(); ++i) {
            if (total_stolen >= max_count) break;

            size_t idx = (start_index + i) % active_producers_.size();
            auto producer = active_producers_[idx];

            if (producer) {
                size_t stolen = 0;
                // dequeue each producer queue
                for (size_t j = 0; j < max_count - total_stolen; ++j) {
                    if (producer->queue->consume_one(func)) {
                        stolen++;
                    } else {
                        break;
                    }
                }
                if (stolen > 0) {
                    producer->approx_size.fetch_sub(stolen, std::memory_order_relaxed);
                    total_stolen += stolen;
                }
            }
        }

        return total_stolen;
    }

    std::shared_ptr<ProducerInfo> get_or_create_producer_info() {
        std::thread::id tid = std::this_thread::get_id();

        {
            std::shared_lock lock(producers_mutex_);
            auto it = producer_map_.find(tid);
            if (it != producer_map_.end()) {
                return it->second;
            }
        }

        std::unique_lock lock(producers_mutex_);
        auto it = producer_map_.find(tid);
        if (it != producer_map_.end()) {
            return it->second;
        }

        auto producer_info = std::make_shared<ProducerInfo>(initial_queue_size_);

        producer_map_[tid] = producer_info;

        active_producers_.push_back(producer_info);

        static thread_local ThreadExitHelper exit_helper([this, tid]() {
            this->on_thread_exit(tid);
        });

        return producer_info;
    }

    void on_thread_exit(std::thread::id tid) {
        std::unique_lock lock(producers_mutex_);
        auto it = producer_map_.find(tid);
        if (it != producer_map_.end()) {
            active_producers_.erase(
                std::remove(active_producers_.begin(), active_producers_.end(), it->second),
                active_producers_.end());

            producer_map_.erase(it);
        }
    }

    void clear_all_producers() {
        std::unique_lock hash_lock(producers_mutex_);

        for (auto& pair : producer_map_) {
            T value;
            while (pair.second->queue->pop(value)) {}
            pair.second->approx_size.store(0, std::memory_order_relaxed);
        }

        producer_map_.clear();
        active_producers_.clear();
    }
};

template<typename Queue, typename InputIterator>
bool enqueue_bulk(Queue& queue, InputIterator first, size_t count) {
    return queue.enqueue_bulk(first, count);
}

template<typename Queue, typename T>
size_t dequeue_bulk(Queue& queue, std::function<void(T)> &&func, size_t max_count) {
    return queue.dequeue_bulk(func, max_count);
}

} // namespace fast_queue