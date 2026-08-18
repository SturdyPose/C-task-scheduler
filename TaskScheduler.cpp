#include "TaskScheduler.h"

#include <format>
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
        : m_repeatableAffinityJobs(std::clamp(nThreads, 1u, std::thread::hardware_concurrency()))
        , m_mainThreadId(std::this_thread::get_id())
    {
        std::latch sync {nThreads};

        for(uint32_t i = 0; i < m_repeatableAffinityJobs.size(); ++i)
        {
            std::jthread jthread{[=,this, &sync](std::stop_token token)
            {
                std::thread::id currentThreadId = std::this_thread::get_id();
                auto& affinityJobs = m_repeatableAffinityJobs.at(i);
                sync.count_down();
                std::vector<AlignedJob> jobs;
                jobs.reserve(1024);
                while(!token.stop_requested())
                {
                    auto predicate = [&]()
                    {
                        return token.stop_requested() || !m_jobs.IsEmpty() || affinityJobs.HasValuesInBackfill();
                    };

                    const auto nowBefore = std::chrono::steady_clock::now();
                    auto optNextDeadline = affinityJobs.GetNextDeadline();
                    {
                        std::unique_lock ul{m_mtx};
                        if (optNextDeadline.has_value())
                        {
                            m_cond.wait_for(ul, *optNextDeadline, predicate);
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
                    m_jobs.SwapBuffers(jobs);
                    for (AlignedJob &job : jobs)
                        job(currentThreadId);
                    jobs.clear();
                }
            }};

            m_jthreads.emplace_back(std::move(jthread));
        }

        sync.wait();
    }

    void TaskScheduler::AddJob(AlignedJob&& job)
    {
        m_jobs.Push(std::forward<AlignedJob>(job));
        m_cond.notify_one();
    }

    RecurringJobID TaskScheduler::AddRecurringJob(RecurringJob&& job)
    {
        CheckIfOnMainThread();

        // Balance where to insert next job
        auto* affinityJobs = &m_repeatableAffinityJobs.at(0);
        size_t indexContainerSelected = 0;
        for(size_t i = 1; i < m_repeatableAffinityJobs.size(); ++i)
        {
            auto& currentAffinityJobs = m_repeatableAffinityJobs[i];
            if(currentAffinityJobs.Size() < affinityJobs->Size())
            {
                affinityJobs = &currentAffinityJobs;
                indexContainerSelected = i; }
        }

        RecurringJobHandler jobHandler = affinityJobs->AddJob(std::move(job));
        m_cond.notify_all();

        return {jobHandler, indexContainerSelected};
    }

    RecurringJobID TaskScheduler::AddRecurringJob(Job&& job, std::chrono::milliseconds everyMs)
    {
        return AddRecurringJob(RecurringJob{std::move(job), everyMs});
    }

    void TaskScheduler::RemoveRecurringJob(RecurringJobID recurringJobID)
    {
        CheckIfOnMainThread();
        auto& affinityJobs = m_repeatableAffinityJobs.at(recurringJobID.whichContainerIndex);
        affinityJobs.RemoveJob(recurringJobID.handler);

        m_cond.notify_all();
    }
    
    void TaskScheduler::CheckIfOnMainThread() const
    {
        std::thread::id threadID = std::this_thread::get_id();
        if(threadID == m_mainThreadId)
        {
            return;
        }
        std::string error = std::format("TaskScheduler::AddRecurringJob should be called only from main thread! Called from {} , and main thread id is: {}", threadID, m_mainThreadId);
        throw CallNotFromMainThreadException(error);
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
            m_job = nullptr;
        }
        m_lastExecuted = std::chrono::steady_clock::now();
    }

    void RecurringJob::Abort()
    {
        m_isAborted = true;
        m_job = Job(); // Reset state to release any heap allocs in std::function
    }


    static std::unique_ptr<TaskScheduler> s_pTaskScheduler = nullptr;
    void CreateTaskScheduler(uint32_t nThreads)
    {
        s_pTaskScheduler = std::make_unique<TaskScheduler>(nThreads);
    }

    TaskScheduler& GetTaskScheduler()
    {
        return *s_pTaskScheduler;
    }
}