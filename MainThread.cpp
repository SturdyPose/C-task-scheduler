#include "MainThread.h"

#include <thread>

namespace Threading
{
    void MainThread::AddJob(AlignedJob &&job) noexcept
    {
        m_vector.Push(std::move(job));
    };

    void MainThread::ProcessJobs()
    {
        std::vector<AlignedJob> jobs;
        jobs.reserve(m_vector.Size());

        m_vector.SwapBuffers(jobs);

        for (AlignedJob &job : jobs)
        {
            try
            {
                job(m_mainThreadId);
            }
            catch(...)
            {
                // TODO: Add identifier to jobs and log crash
            }
        }
    }

    MainThread& MainThread::GetMainThread()
    {
        static MainThread thread{std::this_thread::get_id()};
        return thread;
    }

    MainThread::MainThread(std::thread::id mainThreadId) noexcept
    : m_mainThreadId(mainThreadId)
    {
    }
}