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
   * GCScheduler is a separate scheduler thread pool for garbage collection
   * operations. It uses the generic BackgroundSchedulerThread template 
   * with a GCTag to avoid code duplication.
   */
  struct GCTag
  {
    static constexpr const char* Name = "GC Scheduler Thread";
  };

  using GCScheduler = ThreadPool<BackgroundSchedulerThread<GCTag>>;

  /**
   * Schedule a zero-cown lambda on the GC scheduler pool.
   * This is intended for background garbage collection work.
   */
  template<typename T>
  static void schedule_gc_lambda(T&& f)
  {
    auto w = Closure::make([f = std::forward<T>(f)](Work* w) mutable {
      f();
      return true;
    });
    GCScheduler::schedule(w);
  }

} // namespace verona::rt
