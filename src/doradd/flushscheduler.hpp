// Copyright Microsoft and Project Verona Contributors.
// SPDX-License-Identifier: MIT
#pragma once

#include <sched/backgroundscheduler.h>
#include <sched/behaviour.h>
#include <sched/notification.h>
#include <cpp/vobject.h>

namespace verona::rt
{
  /**
   * FlushScheduler is a separate scheduler thread pool for checkpoint
   * flush operations. It uses the generic BackgroundSchedulerThread template 
   * with a FlushTag to avoid code duplication.
   */
  struct FlushTag
  {
    static constexpr const char* Name = "Flush Scheduler Thread";
  };

  using FlushScheduler = ThreadPool<BackgroundSchedulerThread<FlushTag>>;

  /**
   * Schedule a zero-cown lambda on the Flush scheduler pool.
   * This is intended for checkpoint and I/O-heavy work.
   */
  template<typename T>
  static void schedule_flush_lambda(T&& f)
  {
    auto w = Closure::make([f = std::forward<T>(f)](Work* w) mutable {
      f();
      return true;
    });
    FlushScheduler::schedule(w);
  }

} // namespace verona::rt
