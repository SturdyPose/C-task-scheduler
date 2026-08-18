#pragma once

#include "ThreadSafeContainers.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <format>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <ranges>
#include <stack>
#include <thread>
#include <tuple>
#include <vector>
#include <new>

namespace Threading
{
    class RecurringJobFullException: public std::exception
    {
        std::string message;
    public:
        explicit RecurringJobFullException(const std::string &msg) : message(msg) {}

        const char *what() const noexcept override
        {
            return message.c_str();
        }
    };

    class CallNotFromMainThreadException: public std::exception
    {
        std::string message;
    public:
        explicit CallNotFromMainThreadException(const std::string &msg) : message(msg) {}

        const char *what() const noexcept override
        {
            return message.c_str();
        }
    };


    using Job = std::function<void(std::thread::id)>;

    // TODO: Make AlignedJob take arbitrary arguments
    struct alignas(64) AlignedJob
    {
        template <typename F>
        AlignedJob(F &&f) : m_job(std::forward<F>(f)) {}
        AlignedJob() : m_job(nullptr) {}
        AlignedJob(std::nullptr_t) : m_job(nullptr) {}
        bool operator!=(std::nullptr_t) const { return m_job != nullptr; }
        void operator()(std::thread::id id)
        {
            if (m_job)
                m_job(id);
        }

        private:
        Job m_job; 
    };

    struct RecurringJob final
    {
        RecurringJob() = default;
        RecurringJob(Job&& job, std::chrono::milliseconds every);
        std::optional<std::chrono::milliseconds> GetNextDeadline() const;
        void Execute();
        bool IsAborted() const noexcept;

    private:
        void PerformExecute();
        void Abort();
    
        Job m_job = nullptr;
        std::chrono::milliseconds m_everyNTime{ 1 };
        std::optional<std::chrono::steady_clock::time_point> m_lastExecuted = std::nullopt;
        bool m_isAborted = false;
    };

    using RecurringJobHandler = size_t;

    template <size_t N>
    struct RecurringJobContainer final
    {
        RecurringJobContainer()
        {
            for(size_t i = 0; i < N; ++i)
            {
                m_freeIndices.push(i);
            }
        }

        RecurringJobHandler AddJob(RecurringJob&& job)
        {
            std::unique_lock lock(m_freeIndicesMtx);
            if (m_freeIndices.empty())
            {
                throw RecurringJobFullException(std::format("Can't fit more jobs, {} is the limit", N));
            }

            size_t index = m_freeIndices.top();
            m_freeIndices.pop();
            m_backFillQueue.Push({std::forward<RecurringJob>(job), index});
            return index;
        }

        void RemoveJob(RecurringJobHandler handler)
        {
            std::unique_lock lock(m_freeIndicesMtx);
            m_backFillQueue.Push({{}, handler});
            m_freeIndices.push(handler);
        }

        size_t Size()
        {
            std::unique_lock lock {m_freeIndicesMtx};
            return N - m_freeIndices.size();
        }

        bool IsEmpty()
        {
            return Size() == 0;
        }

        bool HasValuesInBackfill()
        {
            return !m_backFillQueue.IsEmpty();
        }

        void ExecuteJobs()
        {
            std::array<std::pair<RecurringJob, size_t>, N> jobs;
            auto [span1, span2] = m_backFillQueue.SwapBuffers(jobs);
            for (auto& [job, index] : span1)
            {
                m_recurringJobs[index] = std::move(job);
            }
            for (auto& [job, index] : span2)
            {
                m_recurringJobs[index] = std::move(job);
            }

            // Execute jobs as separate loop as it can otherwise have odd timing
            for(RecurringJob& job : m_recurringJobs)
            {
                if(!job.IsAborted()) job.Execute();
            }
        }

        std::optional<std::chrono::milliseconds> GetNextDeadline()
        {
            std::optional<std::chrono::milliseconds> earliest = std::nullopt;
            for (const auto &job : m_recurringJobs)
            {
                auto deadline = job.GetNextDeadline();
                if (!deadline)
                    continue;
                if (!earliest || *deadline < *earliest)
                    earliest = deadline;
            }
            return std::optional<std::chrono::milliseconds>{earliest};
        }

        void Clear()
        {
            m_recurringJobs = {};
        }
        
        private:
        std::array<RecurringJob, N> m_recurringJobs;

        std::mutex m_freeIndicesMtx;
        std::stack<size_t> m_freeIndices;
        // job - index pair
        ThreadSafeCircularQueue<std::pair<RecurringJob, size_t>, N> m_backFillQueue;
    };

    struct RecurringJobID final
    {
        RecurringJobHandler handler;
        size_t whichContainerIndex;
    };

    // Should be constructed only on main thread
    struct TaskScheduler final
    {
        ~TaskScheduler();

        [[nodiscard]] TaskScheduler(uint32_t nThreads) noexcept;
        [[nodiscard]] TaskScheduler(const TaskScheduler&) noexcept = delete;
        [[nodiscard]] TaskScheduler(TaskScheduler&&) noexcept = delete;
        [[nodiscard]] TaskScheduler& operator=(const TaskScheduler&) noexcept = delete;
        [[nodiscard]] TaskScheduler& operator=(TaskScheduler&&) noexcept = delete;

        void AddJob(AlignedJob&& job);
        [[nodiscard]] RecurringJobID AddRecurringJob(RecurringJob&& job);
        [[nodiscard]] RecurringJobID AddRecurringJob(Job&& job, std::chrono::milliseconds everyMs);
        void RemoveRecurringJob(RecurringJobID jobID);

        private:
        void CheckIfOnMainThread() const;

        std::vector<std::jthread> m_jthreads;
        ThreadSafeVector<AlignedJob> m_jobs;

        // Jobs which should be repeated for the specific thread:
        // Each thread manages It's own vector of jobs
        // This container is stable and doesn't resize
        std::vector<RecurringJobContainer<32>> m_repeatableAffinityJobs;

        alignas(64) std::mutex m_mtx;
        std::condition_variable m_cond;
        std::thread::id m_mainThreadId;
    };


    void CreateTaskScheduler(uint32_t nThreads);

    TaskScheduler& GetTaskScheduler();
}