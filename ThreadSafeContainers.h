#pragma once

#include <array>
#include <condition_variable>
#include <cstdint>
#include <format>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
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

template <typename T, size_t N>
struct ThreadSafeCircularQueue final
{
    void Push(T &&item)
    {
        std::unique_lock lg{m_mtx};
        int nextStart = Increment(m_start);
        if (nextStart == m_end)
        {
            throw QueueFullException("ThreadSafeCircularQueue is full");
        }

        m_itemsStorage[nextStart] = std::forward<T>(item);
        m_start = nextStart;

        if (m_end == -1)
            m_end = 0;
    }

    std::optional<T> PopSafe()
    {
        std::unique_lock lg{m_mtx};
        if (m_start == -1)
            return std::nullopt;

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

    void Swap(ThreadSafeCircularQueue<T, N> &queue) noexcept
    {
        if (&queue == this)
            return;

        std::scoped_lock sl{m_mtx, queue.m_mtx};
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
    std::pair<std::span<T>, std::span<T>> SwapBuffers(std::array<T, N> &buffer, int start = -1, int end = -1)
    {
        const int buffSize = static_cast<int>(buffer.size());
        if (start >= buffSize || end >= buffSize)
            throw QueueFullException(std::format("Circular buffer indices can't be bigger than buffer itself start: {} end: {}", start, end));
        std::unique_lock lg{m_mtx};
        m_itemsStorage.swap(buffer);

        std::swap(m_start, start);
        std::swap(m_end, end);

        if (start == -1)
            return {{}, {}};

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

// template <typename T, class Allocator = std::allocator<T>>
// struct ThreadSafeDynamicQueue final
// {
//     void Push(T &&item)
//     {
//         std::unique_lock lg{m_mtx};
//         m_queue.push(std::forward<T>(item));
//     }

//     std::optional<T> PopSafe()
//     {
//         std::unique_lock lg{m_mtx};
//         if (m_queue.empty())
//             return std::nullopt;

//         T temp = std::move(m_queue.front())
//         m_queue.pop();

//         return temp;
//     }

//     bool IsEmpty() const noexcept
//     {
//         return Size() == 0;
//     }

//     int Size() const noexcept
//     {
//         std::unique_lock ul{m_mtx};
//         return m_queue.size();
//     }

//     void Swap(ThreadSafeDynamicQueue<T> &queue) noexcept
//     {
//         if (&queue == this)
//             return;

//         std::scoped_lock sl{m_mtx, queue.m_mtx};
//         m_queue.swap(queue.m_queue);
//     }

//     /*
//     Use SwapBuffers when you want to get whole ownership of insides so Thread doesn't hold on to the
//     resource longer than it needs

//     Example:

//     ThreadSafeCircularQueue<int, 64> queue;
//     *** do something with queue

//     std::queue<int> items;

//     queue.SwapBuffers(items);
//     for(int i : items)
//     {
//         // process the buffer safely without locking threads
//     }

//     */
//     void SwapBuffers(std::queue<T> &buffer)
//     {
//         std::unique_lock lg{m_mtx};
//         m_queue.swap(m_queue);
//     }

// private:
//     std::queue<T> m_queue;
//     mutable std::mutex m_mtx;
// };

template <typename T>
struct ThreadSafeVector final
{
    ThreadSafeVector() = default;
    ThreadSafeVector(const ThreadSafeVector& vector) noexcept
    {
        std::unique_lock ul{vector.m_mtx};
        m_items = vector.m_items;
    }

    ThreadSafeVector(ThreadSafeVector&& vector) noexcept
    {
        std::unique_lock ul{vector.m_mtx};
        m_items = std::move(vector.m_items);
    }

    ThreadSafeVector& operator=(const ThreadSafeVector& vector) noexcept
    {
        if(this == &vector) return *this;
        std::scoped_lock sl{m_mtx, vector.m_mtx};
        m_items = vector.m_items;
        return *this;
    }

    ThreadSafeVector& operator=(ThreadSafeVector&& vector) noexcept
    {
        std::scoped_lock sl{m_mtx, vector.m_mtx};
        m_items = std::move(vector.m_items);
        return *this;
    }

    ~ThreadSafeVector()
    {
        std::unique_lock lg{m_mtx};
        m_items.clear();
    }

    void Push(T &&item)
    {
        std::unique_lock lg{m_mtx};
        m_items.emplace_back(std::forward<T>(item));
    }

    bool IsEmpty() const noexcept
    {
        return Size() == 0;
    }

    int Size() const noexcept
    {
        std::unique_lock ul{m_mtx};
        return m_items.size();
    }

    void Swap(ThreadSafeVector<T> &vector) noexcept
    {
        if (&vector == this)
            return;

        std::scoped_lock sl{m_mtx, vector.m_mtx};
        m_items.swap(vector.m_items);
    }

    /*
    Use SwapBuffers when you want to get whole ownership of insides so Thread doesn't hold on to the
    resource longer than it needs

    Example:

    ThreadSafeVector<int> vec;
    *** do something with vector

    std::vector<int> items;
    vec.SwapBuffers(items);
    for(int i : items)
    {
        // process the buffer safely without locking threads
    }

    */
    void SwapBuffers(std::vector<T> &buffer)
    {
        if(&m_items == &buffer) return;

        std::unique_lock lg{m_mtx};
        m_items.swap(buffer);
    }

private:
    alignas(64) std::vector<T> m_items;
    alignas(64) mutable std::mutex m_mtx;
};

} // namespace Threading