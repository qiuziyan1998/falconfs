/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once


#include <atomic>
#include <chrono>
#include <thread>
#include <memory>
#include <condition_variable>
#include <mutex>

/* V must be trivally copyable */
template<typename V>
class ExpiringCache {
public:
    explicit ExpiringCache(std::chrono::milliseconds ttl) 
        : ttl_(ttl) {
        value_.store(V(), std::memory_order_relaxed);
        isValid_.store(false, std::memory_order_relaxed);
        modifyTimeNs_.store(0, std::memory_order_relaxed);
        /* start timed expire */
        timerThread_ = std::thread(&ExpiringCache::timer_worker, this);
    }

    ~ExpiringCache() {
        {
            std::lock_guard<std::mutex> lock(timerMutex_);
            stop_ = true;
        }
        timerCv_.notify_all();
        if (timerThread_.joinable()) {
            timerThread_.join();
        }
    }

    ExpiringCache(const ExpiringCache&) = delete;
    ExpiringCache& operator=(const ExpiringCache&) = delete;

    bool get(V& value) const;
    void update(V new_value);

private:
    const std::chrono::milliseconds ttl_;

    std::atomic<V> value_;
    std::atomic<bool> isValid_;
    std::atomic<int64_t> modifyTimeNs_;

    std::thread timerThread_;
    std::mutex timerMutex_;
    std::condition_variable timerCv_;
    bool stop_ = false;

    int64_t get_current_time_ns() const;

    std::chrono::steady_clock::time_point ns_to_time_point(int64_t ns) const;

    void timer_worker();
};


template<typename V>
inline bool ExpiringCache<V>::get(V& value) const {
    if (isValid_.load(std::memory_order_acquire)) {
        value = value_.load(std::memory_order_acquire);
        return true;
    }
    return false;
}

template<typename V>
inline void ExpiringCache<V>::update(V new_value) {
    value_.store(new_value, std::memory_order_relaxed);
    auto ns = get_current_time_ns();
    modifyTimeNs_.store(ns, std::memory_order_release);
    /* isValid be the last to update */
    bool old_valid = isValid_.exchange(true, std::memory_order_release);
    
    if (!old_valid) {
        /* no need to hold lock, timer thread will check isValid */
        timerCv_.notify_one();
    }
}

/* from std::chrono::time_point, aka long long, to long */
template<typename V>
inline int64_t ExpiringCache<V>::get_current_time_ns() const {
    auto now = std::chrono::steady_clock::now();
    auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto epoch = now_ns.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count();
}

template<typename V>
inline std::chrono::steady_clock::time_point ExpiringCache<V>::ns_to_time_point(int64_t ns) const {
    auto duration = std::chrono::nanoseconds(ns);
    return std::chrono::steady_clock::time_point(duration);
}

template<typename V>
inline void ExpiringCache<V>::timer_worker() {
    while (!stop_) {
        std::unique_lock<std::mutex> lock(timerMutex_);

        /* wait until isValid turned to true */
        timerCv_.wait(lock, [this]() {
            return stop_ || isValid_.load(std::memory_order_acquire);
        });

        if (stop_) break;

        while (!stop_) {
            /* modify time before wait */
            auto start_modify_time_ns = modifyTimeNs_.load(std::memory_order_acquire);
            auto now_ns = get_current_time_ns();
            /* elapsed time since modify */
            auto elapsed_ns = now_ns - start_modify_time_ns;
            auto ttl_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ttl_).count();

            bool timeOut = true;
            if (elapsed_ns < ttl_ns) {
                auto remaining = std::chrono::nanoseconds(ttl_ns - elapsed_ns);
                timeOut = timerCv_.wait_for(lock, remaining) == std::cv_status::timeout;
            }
            if (timeOut) {
                /* time out, attempt to set expired */
                /* modify_time intact, otherwise touched by update */
                auto current_modify_time_ns = modifyTimeNs_.load(std::memory_order_acquire);
                if (current_modify_time_ns == start_modify_time_ns) {
                    isValid_.store(false, std::memory_order_relaxed);
                    /* value expired, wait for isValid turned true again */
                    break;
                }
                continue;
            } else {
                /* notified, check if need timed wait */
                if (stop_ || !isValid_.load(std::memory_order_acquire)) {
                    break;
                }
            }
        }
    }
}