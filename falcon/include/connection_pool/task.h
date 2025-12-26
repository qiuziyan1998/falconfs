/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_POOLER_TASK_H
#define FALCON_POOLER_TASK_H

#include <brpc/server.h>
#include <vector>
#include <functional>
#include "falcon_meta_rpc.pb.h"
// #include "concurrentqueue/concurrentqueue.h"
#include <boost/lockfree/queue.hpp>

namespace falcon::meta_proto
{

class AsyncMetaServiceJob {
private:
    brpc::Controller *cntl;
    const MetaRequest *request;
    Empty *response;
    google::protobuf::Closure *done;

public:
    AsyncMetaServiceJob(brpc::Controller *cntl,
                        const MetaRequest *request,
                        Empty *response,
                        google::protobuf::Closure *done)
        : cntl(cntl),
          request(request),
          response(response),
          done(done)
    {
    }
    brpc::Controller *GetCntl() { return cntl; }
    const MetaRequest *GetRequest() { return request; }
    Empty *GetResponse() { return response; }
    void Done() { done->Run(); }
};

} // namespace falcon::meta_proto

class JobLockFreeQueue {
public:
    JobLockFreeQueue() = default;
    JobLockFreeQueue(int n) : jobList(n) {}
    boost::lockfree::queue<falcon::meta_proto::AsyncMetaServiceJob *> jobList;
    std::atomic<size_t> m_size;

    bool push(falcon::meta_proto::AsyncMetaServiceJob *job) {
        bool ret = jobList.push(job);
        ++m_size;
        return ret;
    }
    bool pop(falcon::meta_proto::AsyncMetaServiceJob *&job) {
        bool ret = jobList.pop(job);
        --m_size;
        return ret;
    }
    size_t size() {
        return m_size.load();
    }
    void reserve(size_t cap) {
        jobList.reserve(cap);
    }
    // template <typename Functor>
    bool dequeue_one(std::function<void(falcon::meta_proto::AsyncMetaServiceJob *)> && f) {
        bool ret = jobList.consume_one<std::function<void(falcon::meta_proto::AsyncMetaServiceJob *)>>(f);
        --m_size;
        return ret;
    }
};

class Task {
public:
    bool isBatch;
    // moodycamel::ConcurrentQueue<falcon::meta_proto::AsyncMetaServiceJob *> jobList;
    JobLockFreeQueue jobList;
    Task(int n) : jobList(n)
    {
        isBatch = false;
    }
    Task() { isBatch = false; }
};

class WorkerTask {
public:
    bool isBatch;
    std::vector<falcon::meta_proto::AsyncMetaServiceJob *> jobList;
    WorkerTask() { isBatch = false; }
};

#endif
