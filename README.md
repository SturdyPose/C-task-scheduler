# C++23 task scheduler
Task scheduler using C++23 standard library


## Usage example:

```cpp
#include "TaskScheduler.h"

#include <functional>
#include <iostream>
#include <latch>
#include <format>

int main(int argc, char *argv[])
{
    {
        Threading::TaskScheduler scheduler{4};
        // Will wait until all threads initialized

        std::latch latch{1024};
        for(int i = 0; i < 1024; ++i)
        {
            scheduler.AddJob([&latch](std::thread::id threadId){
                std::cout << std::format("Called from {}\n", threadId);
                latch.count_down();
            });
            // Sleep is just to see that threads vary
            std::this_thread::sleep_for(std::chrono::microseconds(250));
        }
        // Wait for all jobs to finish
        latch.wait();

    } // Will wait until ~TaskScheduler is destroyed

    std::cout << "Finished!" << std::endl;
    return 0;
}
```
