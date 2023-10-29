// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.

use std::cell::OnceCell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use deno_core::error::type_error;
use deno_core::error::AnyError;
use deno_core::futures;
use deno_core::futures::FutureExt;
use deno_unsync::spawn;
use deno_unsync::JoinHandle;
use tokio::sync::mpsc;
use tokio::sync::mpsc::WeakSender;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::CronHandle;
use crate::CronHandler;
use crate::CronSpec;

const MAX_CRONS: usize = 100;
const DISPATCH_CONCURRENCY_LIMIT: usize = 50;
const MAX_BACKOFF_MS: u32 = 60 * 60 * 1_000; // 1 hour
const MAX_BACKOFF_COUNT: usize = 5;
const DEFAULT_BACKOFF_SCHEDULE: [u32; 5] = [100, 1_000, 5_000, 30_000, 60_000];

pub struct LocalCronHandler {
  inner: Rc<RefCell<Inner>>,
}

struct Inner {
  crons: HashMap<String, CronJob>,
  scheduled_deadlines: BTreeMap<u64, Vec<String>>,
  cron_schedule_tx: OnceCell<mpsc::Sender<(String, bool)>>,
  concurrency_limiter: Arc<Semaphore>,
  cron_loop_join_handle: OnceCell<JoinHandle<()>>,
}

struct CronJob {
  spec: CronSpec,
  next_tx: mpsc::WeakSender<()>,
  current_execution_retries: u32,
}

impl CronJob {
  fn backoff_schedule(&self) -> &[u32] {
    self
      .spec
      .backoff_schedule
      .as_deref()
      .unwrap_or(&DEFAULT_BACKOFF_SCHEDULE)
  }
}

impl LocalCronHandler {
  pub fn new() -> Option<Self> {
    Some(Self {
      inner: Rc::new(RefCell::new(Inner {
        crons: HashMap::new(),
        scheduled_deadlines: BTreeMap::new(),
        cron_schedule_tx: OnceCell::new(),
        concurrency_limiter: Arc::new(Semaphore::new(
          DISPATCH_CONCURRENCY_LIMIT,
        )),
        cron_loop_join_handle: OnceCell::new(),
      })),
    })
  }
}

impl Inner {
  fn get_ready_crons(
    &mut self,
  ) -> Result<Vec<(String, WeakSender<()>)>, AnyError> {
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;

    // Get crons that need to execute right now.
    let crons_to_execute = {
      let to_remove = self
        .scheduled_deadlines
        .range(..=now)
        .map(|(ts, _)| *ts)
        .collect::<Vec<_>>();
      to_remove
        .iter()
        .flat_map(|ts| {
          self
            .scheduled_deadlines
            .remove(ts)
            .unwrap()
            .iter()
            .map(move |name| (*ts, name.clone()))
            .collect::<Vec<_>>()
        })
        .map(|(_, name)| {
          (name.clone(), self.crons.get(&name).unwrap().next_tx.clone())
        })
        .collect::<Vec<_>>()
    };

    Ok(crons_to_execute)
  }

  async fn cron_loop(
    self_rc: Rc<RefCell<Self>>,
    mut cron_schedule_rx: mpsc::Receiver<(String, bool)>,
  ) -> Result<(), AnyError> {
    loop {
      // Find the earliest ts and sleep until then.
      let earliest_deadline =
        { self_rc.borrow().scheduled_deadlines.keys().next().copied() };

      let sleep_fut = if let Some(earliest_deadline) = earliest_deadline {
        let now = SystemTime::now()
          .duration_since(SystemTime::UNIX_EPOCH)
          .unwrap()
          .as_millis() as u64;
        if earliest_deadline <= now {
          continue;
        } else {
          tokio::time::sleep(std::time::Duration::from_millis(
            earliest_deadline - now,
          ))
          .boxed()
        }
      } else {
        futures::future::pending().boxed()
      };

      let cron_to_schedule = tokio::select! {
        _ = sleep_fut => None,
        x = cron_schedule_rx.recv() => {
          if x.is_none() {
            return Ok(());
          };
          x
        }
      };

      // Schedule the cron.
      if let Some((name, prev_success)) = cron_to_schedule {
        let self_ = &mut *self_rc.borrow_mut();
        if let Some(cron) = self_.crons.get_mut(&name) {
          let backoff_schedule = cron.backoff_schedule();
          let next_deadline = if !prev_success
            && cron.current_execution_retries < backoff_schedule.len() as u32
          {
            let backoff_ms =
              backoff_schedule[cron.current_execution_retries as usize];
            let now = SystemTime::now()
              .duration_since(SystemTime::UNIX_EPOCH)
              .unwrap()
              .as_millis() as u64;
            cron.current_execution_retries += 1;
            now + backoff_ms as u64
          } else {
            let next_ts = compute_next_deadline(&cron.spec.cron_schedule)?;
            cron.current_execution_retries = 0;
            next_ts
          };
          self_
            .scheduled_deadlines
            .entry(next_deadline)
            .or_default()
            .push(name.to_string());
        }
      }

      // Dispatch ready to execute crons.
      let crons_to_execute = {
        let self_ = &mut *self_rc.borrow_mut();
        self_.get_ready_crons()?
      };
      for (_, tx) in crons_to_execute {
        if let Some(tx) = tx.upgrade() {
          let _ = tx.send(()).await;
        }
      }
    }
  }
}

#[async_trait(?Send)]
impl CronHandler for LocalCronHandler {
  type EH = CronExecutionHandle;

  fn create(&self, spec: CronSpec) -> Result<Self::EH, AnyError> {
    let mut inner = self.inner.borrow_mut();
    let inner = &mut *inner;

    if inner.crons.len() > MAX_CRONS {
      return Err(type_error("Too many crons"));
    }
    if inner.crons.contains_key(&spec.name) {
      return Err(type_error("Cron with this name already exists"));
    }

    // Validate schedule expression.
    spec
      .cron_schedule
      .parse::<saffron::Cron>()
      .map_err(|_| type_error("Invalid cron schedule"))?;

    // Validate backoff_schedule.
    if let Some(backoff_schedule) = &spec.backoff_schedule {
      validate_backoff_schedule(backoff_schedule)?;
    }

    let (next_tx, next_rx) = mpsc::channel::<()>(1);
    let cron = CronJob {
      spec: spec.clone(),
      next_tx: next_tx.downgrade(),
      current_execution_retries: 0,
    };
    inner.crons.insert(spec.name.clone(), cron);

    // Ensure that the cron loop is started.
    inner.cron_loop_join_handle.get_or_init(|| {
      let (cron_schedule_tx, cron_schedule_rx) =
        mpsc::channel::<(String, bool)>(1);
      inner.cron_schedule_tx.set(cron_schedule_tx).unwrap();
      let inner = self.inner.clone();
      spawn(async move {
        Inner::cron_loop(inner, cron_schedule_rx).await.unwrap();
      })
    });

    Ok(CronExecutionHandle {
      name: spec.name.clone(),
      cron_schedule_tx: inner.cron_schedule_tx.get().unwrap().clone(),
      concurrency_limiter: inner.concurrency_limiter.clone(),
      inner_handler: Rc::downgrade(&self.inner),
      inner: RefCell::new(HandleInner {
        next_rx: Some(next_rx),
        shutdown_tx: Some(next_tx),
        permit: None,
      }),
    })
  }
}

pub struct CronExecutionHandle {
  name: String,
  inner_handler: Weak<RefCell<Inner>>,
  inner: RefCell<HandleInner>,
  cron_schedule_tx: mpsc::Sender<(String, bool)>,
  concurrency_limiter: Arc<Semaphore>,
}

struct HandleInner {
  next_rx: Option<mpsc::Receiver<()>>,
  shutdown_tx: Option<mpsc::Sender<()>>,
  permit: Option<OwnedSemaphorePermit>,
}

#[async_trait(?Send)]
impl CronHandle for CronExecutionHandle {
  async fn next(&self, prev_success: bool) -> Result<bool, AnyError> {
    //let inner = self.inner.borrow();
    self.inner.borrow_mut().permit.take();

    // Notify the cron loop and wait for the next execution.
    if self
      .cron_schedule_tx
      .send((self.name.clone(), prev_success))
      .await
      .is_err()
    {
      return Ok(false);
    };
    let Some(mut next_rx) = self.inner.borrow_mut().next_rx.take() else {
      return Ok(false);
    };
    if next_rx.recv().await.is_none() {
      return Ok(false);
    };

    let permit = self.concurrency_limiter.clone().acquire_owned().await?;
    let inner = &mut *self.inner.borrow_mut();
    inner.next_rx = Some(next_rx);
    inner.permit = Some(permit);
    Ok(true)
  }

  fn close(&self) {
    if let Some(tx) = self.inner.borrow_mut().shutdown_tx.take() {
      drop(tx)
    }
    if let Some(inner_handler) = self.inner_handler.upgrade() {
      let mut inner_handler = inner_handler.borrow_mut();
      inner_handler.crons.remove(&self.name);
    }
  }
}

fn compute_next_deadline(cron_expression: &str) -> Result<u64, AnyError> {
  let cron = cron_expression
    .parse::<saffron::Cron>()
    .map_err(|_| anyhow::anyhow!("invalid cron expression"))?;

  let now = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64;

  if let Ok(test_schedule) = env::var("DENO_CRON_TEST_SCHEDULE") {
    if let Ok(offset) = test_schedule.parse::<u64>() {
      return Ok(now + offset);
    }
  }

  let Some(now) = NaiveDateTime::from_timestamp_opt((now / 1_000) as i64, 0)
  else {
    return Err(anyhow::anyhow!("invalid last_deadline_ms"));
  };
  let now = Utc.from_utc_datetime(&now);

  let Some(next_deadline) = cron.next_after(now) else {
    return Err(anyhow::anyhow!("invalid cron expression"));
  };
  Ok((next_deadline.timestamp() * 1_000) as u64)
}

fn validate_backoff_schedule(
  backoff_schedule: &Vec<u32>,
) -> Result<(), AnyError> {
  if backoff_schedule.len() > MAX_BACKOFF_COUNT {
    return Err(type_error("Invalid backoff schedule"));
  }
  if backoff_schedule.iter().any(|s| *s > MAX_BACKOFF_MS) {
    return Err(type_error("Invalid backoff schedule"));
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_compute_next_deadline() {
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    assert!(compute_next_deadline("*/1 * * * *").unwrap() > now);
    assert!(compute_next_deadline("* * * * *").unwrap() > now);
    assert!(compute_next_deadline("bogus").is_err());
    assert!(compute_next_deadline("* * * * * *").is_err());
    assert!(compute_next_deadline("* * *").is_err());
  }
}
