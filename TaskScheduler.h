#pragma once

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

namespace Threading
{
    class QueueFullException: public std::exception
    {
        std::string message;
    public:
        explicit QueueFullException(const std::string &msg) : message(msg) {}

        const char *what() const noexcept override
        {
            return message.c_str();
        }
    };

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

    template <typename T, size_t N>
    struct ThreadSafeCircularQueue final
    {
        void Push(T&& item)
        {
            std::unique_lock lg{m_mtx};
            int nextStart = Increment(m_start);
            if(nextStart == m_end)
            {
                throw QueueFullException("ThreadSafeCircularQueue is full");
            }

            m_itemsStorage[nextStart] = std::forward<T>(item);
            m_start = nextStart;

            if (m_end == -1) m_end = 0;
        }

        std::optional<T> PopSafe()
        {
            std::unique_lock lg{m_mtx};
            if (m_start == -1) return std::nullopt;

            int nextEnd = Increment(m_end);
            T temp = std::move(m_itemsStorage[m_end]);

            if (m_start == m_end)
            {
                m_start = -1;
                m_end = -1;
            }
            else
            {
                m_end = nextEnd;
            }

            return temp;
        }

        bool IsEmpty() const noexcept 
        {
            return Size() == 0;
        }

        int Size() const noexcept 
        {
            std::unique_lock ul{m_mtx};
            if (m_start == -1)
                return 0;

            return (m_start - m_end + static_cast<int>(N)) % static_cast<int>(N) + 1;
        }

        void Swap(ThreadSafeCircularQueue<T, N>& queue) noexcept
        {
            if(&queue == this) return;

            std::scoped_lock sl {m_mtx, queue.m_mtx};
            m_itemsStorage.swap(queue.m_itemsStorage);
            std::swap(queue.m_end, m_end);
            std::swap(queue.m_start, m_start);
        }

        /*
        Use SwapBuffers when you want to get whole ownership of insides so Thread doesn't hold on to the
        resource longer than it needs

        Example:

        ThreadSafeCircularQueue<int, 64> queue;
        *** do something with queue

        std::array<int, 64> someBuff;

        Because circular queue is disjointed, you'll have two spans
        auto [span1, span2] = queue.SwapBuffers(someBuff);

        for(int i : span1)
        {
            // process the buffer safely without locking threads 
        }
        for(int i : span2)
        {
            // process the buffer safely without locking threads 
        }

        For future: c++26 will have std::views::concat available
        */
        std::pair<std::span<T>, std::span<T>> SwapBuffers(std::array<T, N>& buffer, int start = -1, int end = -1)
        {
            const int buffSize = static_cast<int>(buffer.size());
            if(start >= buffSize || end >= buffSize) throw QueueFullException(std::format("Circular buffer indices can't be bigger than buffer itself start: {} end: {}", start, end));
            std::unique_lock lg{m_mtx};
            m_itemsStorage.swap(buffer);

            std::swap(m_start, start);
            std::swap(m_end, end);

            if (start == -1) return {{}, {}};

            if (start >= end)
            {
                return {
                    std::span<T>{buffer.begin() + (end), buffer.begin() + (start + 1)},
                    std::span<T>{}};
            }
            else
            {
                return {
                    std::span<T>{buffer.begin() + end, buffer.end()},
                    std::span<T>{buffer.begin(), buffer.begin() + (start + 1)}};
            }
        }

        private: 
        int Increment(int val) const noexcept
        {
            return (val == -1) ? 0 : (val + 1) % static_cast<int>(N);
        }

        std::array<T, N> m_itemsStorage;

        int m_start = -1;
        int m_end = -1;

        mutable std::mutex m_mtx;
    };

    using Job = std::function<void(std::thread::id)>;

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

    template <size_t N>
    struct RecurringJobContainer final
    {
        using RecurringJobHandler = size_t;

        RecurringJobContainer()
        {
            for(size_t i = 0; i < N; ++i)
            {
                m_freeIndices.push(i);
            }
        }

        RecurringJobHandler AddJob(RecurringJob&& job)
        {
            if (m_freeIndices.empty())
            {
                throw RecurringJobFullException(std::format("Can't fit more jobs, {} is the limit", N));
            }

            std::unique_lock lock(m_freeIndicesMtx);
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
            std::array<std::pair<RecurringJob, size_t>, 16> jobs;
            auto [span1, span2] = m_backFillQueue.SwapBuffers(jobs);
            for (auto& [job, index] : span1)
            {
                m_recurringJobs[index] = std::move(job);
            }
            for (auto& [job, index] : span2)
            {
                m_recurringJobs[index] = std::move(job);
            }

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
        ThreadSafeCircularQueue<std::pair<RecurringJob, size_t>, 16> m_backFillQueue;
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

        void AddJob(Job&& job);
        void AddRecurringJob(RecurringJob&& job);
        void AddRecurringJob(Job&& job, std::chrono::milliseconds everyMs);

        private:
        std::vector<std::jthread> m_jthreads;
        ThreadSafeCircularQueue<Job, 1024> m_queue;

        // Jobs which should be repeated for the specific thread
        // Each thread manages It's own vector of jobs
        std::vector<RecurringJobContainer<32>> m_repeatableAffinityJobs;

        std::mutex m_mtx;
        std::condition_variable m_cond;
        std::thread::id m_mainThreadId;
    };

}