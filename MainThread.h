#pragma once

#include "ThreadSafeContainers.h"
#include "TaskScheduler.h"

#include <memory>
#include <thread>

namespace Threading
{
    struct MainThread final
    {
        void AddJob(AlignedJob&& job) noexcept;
        void ProcessJobs();
        static MainThread& GetMainThread();

        private:
        MainThread(std::thread::id mainThreadId) noexcept;

        std::thread::id m_mainThreadId;
        ThreadSafeVector<AlignedJob> m_vector;
    };
}