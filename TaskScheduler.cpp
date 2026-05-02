#include "TaskScheduler.h"

#include <latch>
#include <stop_token>
#include <thread>

namespace Threading
{
    TaskScheduler::~TaskScheduler()
    {
        {
            std::lock_guard lock(m_mtx);
            for (auto &jthread : m_jthreads)
            {
                jthread.request_stop();
            }
        }
        m_cond.notify_all();
        for (auto &jthread : m_jthreads)
        {
            if (jthread.joinable())
                jthread.join();
        }
    }

    TaskScheduler::TaskScheduler(uint32_t nThreads) noexcept
        : m_repeatableAffinityJobs(nThreads)
        , m_mainThreadId(std::this_thread::get_id())
    {
        std::latch sync {nThreads};

        for(uint32_t i = 0; i < nThreads; ++i)
        {
            std::jthread jthread{[=,this, &sync](std::stop_token token)
            {
                std::thread::id currentThreadId = std::this_thread::get_id();
                auto& affinityJobs = m_repeatableAffinityJobs.at(i);
                sync.count_down();
                while(!token.stop_requested())
                {
                    auto predicate = [&]()
                    {
                        return token.stop_requested() || !m_queue.IsEmpty() || affinityJobs.HasValuesInBackfill();
                    };

                    const auto nowBefore = std::chrono::steady_clock::now();
                    auto nextDeadline = affinityJobs.GetNextDeadline();
                    {
                        std::unique_lock ul{m_mtx};
                        if (nextDeadline.has_value())
                        {
                            // Wait until the next job is due OR a new global job arrives
                            const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - nowBefore);
                            const auto waitingDeadline = *nextDeadline - diff;
                            m_cond.wait_for(ul, *nextDeadline - diff, predicate);
                        }
                        else
                        {
                            // No recurring jobs, wait indefinitely for a global job
                            m_cond.wait(ul, predicate);
                        }
                    }

                    if (token.stop_requested())
                        break;
                    affinityJobs.ExecuteJobs();
                    
                    // Get all the jobs for the current thread
                    std::array<Job, 64> jobQueue;
                    auto [span1, span2] = m_queue.SwapBuffers(jobQueue);
                    for (Job &job : span1)
                        job(currentThreadId);
                    for (Job &job : span2)
                        job(currentThreadId);
                }
            }};

            m_jthreads.emplace_back(std::move(jthread));
        }

        sync.wait();
    }

    void TaskScheduler::AddJob(Job&& job)
    {
        {
            std::unique_lock ul {m_mtx};
            m_queue.Push(std::forward<Job>(job));
        }
        m_cond.notify_one();
    }

    void TaskScheduler::AddRecurringJob(RecurringJob&& job)
    {
        std::thread::id threadID = std::this_thread::get_id();
        if(threadID != m_mainThreadId)
        {
            throw CallNotFromMainThreadException(std::format("TaskScheduler::AddRecurringJob should be called only from main thread! Called from {} , and main thread id is: {}", threadID, m_mainThreadId));
        }

        auto* affinityJobs = &m_repeatableAffinityJobs.at(0);
        for(auto it = m_repeatableAffinityJobs.begin() + 1; it != m_repeatableAffinityJobs.end(); ++it)
        {
            auto& currentAffinityJobs = *it;
            if(currentAffinityJobs.Size() < affinityJobs->Size())
            {
                affinityJobs = &currentAffinityJobs;
            }
        }

        {
            std::unique_lock ul {m_mtx};
            affinityJobs->AddJob(std::move(job));
        }
        m_cond.notify_all();
    }

    void TaskScheduler::AddRecurringJob(Job&& job, std::chrono::milliseconds everyMs)
    {
        AddRecurringJob(RecurringJob{std::move(job), everyMs});
    }

    RecurringJob::RecurringJob(Job &&job, std::chrono::milliseconds every)
        : m_job(std::forward<Job>(job)), m_everyNTime(every)
    {
    }

    std::optional<std::chrono::milliseconds> RecurringJob::GetNextDeadline() const
    {
        if (IsAborted())
            return std::nullopt;
        return m_everyNTime;
    }

    void RecurringJob::Execute()
    {
        if (IsAborted())
        {
            return;
        }

        if (!m_lastExecuted.has_value())
        {
            PerformExecute();
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        std::chrono::milliseconds elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration(now - *m_lastExecuted));
        if (elapsed < m_everyNTime)
            return;

        PerformExecute();
    }

    bool RecurringJob::IsAborted() const noexcept
    {
        return m_isAborted || !m_job;
    }

    void RecurringJob::PerformExecute()
    {
        const std::thread::id thisThreadId = std::this_thread::get_id();
        try
        {
            m_job(thisThreadId);
        }
        catch (...)
        {
            // TODO: maybe add stack trace report here, hmmm
            m_isAborted = true;
        }
        m_lastExecuted = std::chrono::steady_clock::now();
    }

    void RecurringJob::Abort()
    {
        m_isAborted = true;
        m_job = Job(); // Reset state to release any heap allocs in std::function
    }
}