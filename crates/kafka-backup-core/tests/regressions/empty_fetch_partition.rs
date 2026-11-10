mod empty_fetch_regressions {
    use super::*;

    fn options(budget_secs: u64) -> BackupOptions {
        BackupOptions {
            empty_fetch_retry_secs: budget_secs,
            ..small_segments(1000)
        }
    }

    fn empty(offset: i64, end: i64) -> FetchResponse {
        FetchResponse {
            next_offset: offset,
            high_watermark: end,
            last_stable_offset: end,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn retry_must_not_archive_records_appended_after_captured_end() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![Ok(empty(0, 3)), Ok(response(vec![0, 1, 2, 3, 4], 5))],
            (0, 5),
        ));
        context(&h, f, range(0, 0, 3), options(10), CancellationToken::new())
            .backup_partition()
            .await
            .unwrap();
        h.persistence.end_cycle().await.unwrap();
        assert_eq!(stored_manifest(&h.storage).await.total_records(), 3);
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn advancing_empty_fetch_cannot_checkpoint_past_captured_end() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::new(vec![Ok((vec![], 10))], (0, 10)));
        context(&h, f, range(0, 0, 3), options(10), CancellationToken::new())
            .backup_partition()
            .await
            .unwrap();
        h.persistence.end_cycle().await.unwrap();
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn transient_empty_below_target_must_not_complete_without_data() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![Ok(empty(0, 3)), Ok(response(vec![0, 1, 2], 3))],
            (0, 3),
        ));
        context(
            &h,
            f.clone(),
            range(0, 0, 3),
            options(10),
            CancellationToken::new(),
        )
        .backup_partition()
        .await
        .unwrap();
        h.persistence.end_cycle().await.unwrap();
        let m = stored_manifest(&h.storage).await;
        assert_eq!(
            m.total_records(),
            3,
            "must not silently succeed with records still readable"
        );
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn zero_budget_fails_without_checkpointing_unread_data() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![Ok(empty(0, 3)), Ok(response(vec![0, 1, 2], 3))],
            (0, 3),
        ));
        context(
            &h,
            f.clone(),
            range(0, 0, 3),
            options(0),
            CancellationToken::new(),
        )
        .backup_partition()
        .await
        .expect_err("zero budget must fail closed");
        h.persistence.end_cycle().await.unwrap();
        assert_eq!(stored_manifest(&h.storage).await.total_records(), 0);
        assert_eq!(f.fetches.load(Ordering::SeqCst), 1);
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), None);
    }

    struct DelayedSecondFetch {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl PartitionFetcher for DelayedSecondFetch {
        async fn fetch_partition(
            &self,
            _: &str,
            _: i32,
            _: i64,
            _: i32,
            _: IsolationLevel,
        ) -> Result<FetchResponse> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(empty(0, 1));
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(response(vec![0], 1))
        }
        async fn partition_offsets(
            &self,
            _: &str,
            _: i32,
            _: IsolationLevel,
        ) -> Result<(i64, i64)> {
            Ok((0, 1))
        }
    }

    #[tokio::test]
    async fn retry_budget_must_bound_slow_fetch_after_backoff() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(DelayedSecondFetch {
            calls: AtomicUsize::new(0),
        });
        let start = Instant::now();
        let result = context(
            &h,
            f.clone(),
            range(0, 0, 1),
            options(1),
            CancellationToken::new(),
        )
        .backup_partition()
        .await;
        assert_eq!(
            f.calls.load(Ordering::SeqCst),
            1,
            "deadline must prevent a new RPC"
        );
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(1800),
            "1-second retry budget consumed {elapsed:?}, result={result:?}, fetches={}",
            f.calls.load(Ordering::SeqCst)
        );
        assert!(
            result.is_err(),
            "unserved range at deadline must be incomplete, not success"
        );
    }

    #[tokio::test]
    async fn deadline_cancels_an_in_flight_retry() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(DelayedSecondFetch {
            calls: AtomicUsize::new(0),
        });
        let start = Instant::now();
        let result = context(
            &h,
            f.clone(),
            range(0, 0, 1),
            options(2),
            CancellationToken::new(),
        )
        .backup_partition()
        .await;
        assert!(
            result.is_err(),
            "late data must not turn a timed-out backup into success"
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 2);
        assert!(start.elapsed() < Duration::from_millis(2800));
    }

    #[tokio::test]
    async fn progress_with_no_records_must_not_enter_empty_retry() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::new(vec![Ok((vec![], 10))], (0, 10)));
        let start = Instant::now();
        context(
            &h,
            f.clone(),
            range(0, 0, 10),
            options(10),
            CancellationToken::new(),
        )
        .backup_partition()
        .await
        .unwrap();
        h.persistence.end_cycle().await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(800));
        assert_eq!(f.fetches.load(Ordering::SeqCst), 1);
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(9));
    }

    #[tokio::test]
    async fn caught_up_partition_must_not_fetch_or_sleep() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::new(vec![], (0, 0)));
        context(
            &h,
            f.clone(),
            range(0, 0, 0),
            options(10),
            CancellationToken::new(),
        )
        .backup_partition()
        .await
        .unwrap();
        assert_eq!(f.fetches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_interrupts_empty_fetch_backoff() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![Ok(empty(0, 3))],
            (0, 3),
        ));
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            signal.cancel();
        });
        let start = Instant::now();
        context(&h, f.clone(), range(0, 0, 3), options(30), cancel)
            .backup_partition()
            .await
            .unwrap();
        task.abort();
        assert!(start.elapsed() < Duration::from_millis(800));
        assert_eq!(f.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn progress_between_empty_streaks_resets_the_budget() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![
                Ok(empty(0, 2)),
                Ok(response(vec![0], 1)),
                Ok(empty(1, 2)),
                Ok(response(vec![1], 2)),
            ],
            (0, 2),
        ));
        context(&h, f, range(0, 0, 2), options(2), CancellationToken::new())
            .backup_partition()
            .await
            .unwrap();
        h.persistence.end_cycle().await.unwrap();
        assert_eq!(stored_manifest(&h.storage).await.total_records(), 2);
    }

    #[tokio::test]
    async fn exhaustion_flushes_records_without_checkpointing_unread_offsets() {
        let h = harness(Duration::from_secs(3600)).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![
                Ok(response(vec![0, 1], 2)),
                Ok(empty(2, 4)),
                Ok(empty(2, 4)),
            ],
            (0, 4),
        ));
        let result = context(&h, f, range(0, 0, 4), options(1), CancellationToken::new())
            .backup_partition()
            .await;
        assert!(
            result.is_err(),
            "remaining unread offsets must fail the partition"
        );
        let m = stored_manifest(&h.storage).await;
        assert_eq!(m.total_records(), 2);
        assert_eq!(reach_of(&m, 0), Some(1));
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(1));
        assert_eq!(
            m.total_gaps(),
            0,
            "empty does not prove permanent data loss"
        );
    }

    #[tokio::test]
    async fn retention_offset_progress_must_reset_empty_streak_budget() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![
                Ok(empty(0, 3)),
                Ok(empty(0, 3)),
                Err(Error::Kafka(KafkaError::BrokerError {
                    code: 1,
                    message: "OFFSET_OUT_OF_RANGE".into(),
                })),
                Ok(empty(2, 3)),
                Ok(empty(2, 3)),
                Ok(response(vec![2], 3)),
            ],
            (2, 3),
        ));
        let result = context(
            &h,
            f.clone(),
            range(0, 0, 3),
            options(4),
            CancellationToken::new(),
        )
        .backup_partition()
        .await;
        assert!(result.is_ok(), "retention moved offset 0 to 2; its new empty streak deserves a fresh budget: {result:?}; fetches={}", f.fetches.load(Ordering::SeqCst));
        h.persistence.end_cycle().await.unwrap();
        let m = stored_manifest(&h.storage).await;
        assert_eq!(m.total_records(), 1);
        assert_eq!(m.total_gaps(), 1);
        assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn protocol_errors_are_not_hidden_by_empty_retries() {
        let h = harness(Duration::ZERO).await;
        let f = Arc::new(ScriptedFetcher::with_responses(
            vec![Err(Error::Kafka(KafkaError::BrokerError {
                code: 29,
                message: "TOPIC_AUTHORIZATION_FAILED".into(),
            }))],
            (0, 3),
        ));
        let result = context(
            &h,
            f.clone(),
            range(0, 0, 3),
            options(10),
            CancellationToken::new(),
        )
        .backup_partition()
        .await;
        assert!(result.is_err());
        assert_eq!(f.fetches.load(Ordering::SeqCst), 1);
    }
}
