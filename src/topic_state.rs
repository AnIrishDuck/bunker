use crate::topic::SegmentSpan;
use rusqlite::{params, Connection, Result};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use time::OffsetDateTime;

pub struct TopicState {
    path: PathBuf,
    connection: Connection,
}

impl TopicState {
    pub fn attach(path: PathBuf) -> Self {
        let prior = Path::new(&path).exists();
        let connection = Connection::open(&path).unwrap();
        if !prior {
            connection
                .execute(
                    "CREATE TABLE segments (
                        id              INTEGER PRIMARY KEY,
                        partition       STRING,
                        segment_id      INTEGER,
                        time_start      DATETIME,
                        time_end        DATETIME,
                        index_start     INTEGER,
                        index_end     INTEGER
                        )
                        ",
                    [],
                )
                .unwrap();
            connection
                .execute(
                    "CREATE UNIQUE INDEX segments_segment_id ON segments(partition, segment_id)",
                    [],
                )
                .unwrap();
            connection
                .execute(
                    "CREATE INDEX segments_start ON segments(partition, index_start)",
                    [],
                )
                .unwrap();
        }
        TopicState { path, connection }
    }

    pub(crate) fn update(
        &self,
        partition: &str,
        segment_ix: usize,
        ixs: &SegmentSpan<Range<u128>>,
    ) -> () {
        self.connection
            .execute(
                "
            INSERT INTO segments(
                partition,
                segment_id,
                time_start,
                time_end,
                index_start,
                index_end
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(partition, segment_id) DO UPDATE SET
                time_start=excluded.time_start;
                time_end=excluded.time_end;
                index_start=excluded.index_start;
                index_end=excluded.index_end;
        ",
                params![
                    partition,
                    segment_ix,
                    OffsetDateTime::from(*ixs.time.start()),
                    OffsetDateTime::from(*ixs.time.end()),
                    u64::try_from(ixs.index.start).unwrap(),
                    u64::try_from(ixs.index.end).unwrap()
                ],
            )
            .unwrap();
    }

    pub fn get_segment_span(
        &self,
        partition: &str,
        segment_ix: usize,
    ) -> Option<SegmentSpan<Range<u128>>> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT time_start, time_end, index_start, index_end FROM segments
            WHERE partition = ?1 AND segment_id = ?2
        ",
            )
            .unwrap();

        let r = statement
            .query_map(params![partition, segment_ix], |row| {
                Ok(SegmentSpan {
                    time: (SystemTime::from(row.get::<_, OffsetDateTime>(0)?)
                        ..=SystemTime::from(row.get::<_, OffsetDateTime>(1)?)),
                    index: (u128::from(row.get::<_, u64>(2)?)..u128::from(row.get::<_, u64>(3)?)),
                })
            })
            .unwrap()
            .next();

        r.map(|v| v.unwrap())
    }

    pub fn get_segment_for_ix(&self, partition: &str, ix: u64) -> Option<usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT segment_id FROM segments
            WHERE partition = ?1 AND index_start <= ?2 ORDER BY segment_id DESC LIMIT 1
        ",
            )
            .unwrap();

        let r = statement
            .query_map(params![partition, ix], |row| row.get(0))
            .unwrap()
            .next();

        r.map(|v| v.unwrap())
    }

    pub fn get_active_segment(&self, partition: &str) -> Option<usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT segment_id + 1 FROM segments
            WHERE partition = ?1
            ORDER BY segment_id DESC LIMIT 1
        ",
            )
            .unwrap();

        let r = statement
            .query_map([partition], |row| row.get(0))
            .unwrap()
            .next();

        r.map(|v: Result<usize, _>| v.unwrap())
    }

    pub fn get_active_segments(&self) -> HashMap<String, usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT partition, MAX(segment_id) + 1 AS active FROM segments
            GROUP BY partition
        ",
            )
            .unwrap();

        let r = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap();

        r.map(|v: Result<(String, usize), _>| v.unwrap()).collect()
    }
}

mod test {
    use super::*;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[test]
    fn test_update() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = TopicState::attach(PathBuf::from(path));
        assert_eq!(state.get_active_segment("default"), None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update("default", 0, &SegmentSpan { time, index: 0..ix })
        }

        state.update(
            "default",
            1,
            &SegmentSpan {
                time,
                index: 10..20,
            },
        );
        assert_eq!(state.get_segment_for_ix("default", 0), Some(0));
        assert_eq!(state.get_segment_for_ix("default", 5), Some(0));
        assert_eq!(state.get_segment_for_ix("default", 9), Some(0));
        assert_eq!(state.get_segment_for_ix("default", 15), Some(1));
        assert_eq!(state.get_segment_for_ix("default", 200), Some(1));
        assert_eq!(state.get_active_segment("default"), Some(2));
    }

    #[test]
    fn test_partitions() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = TopicState::attach(PathBuf::from(path));
        assert_eq!(state.get_active_segments(), HashMap::new());

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update("a", 0, &SegmentSpan { time, index: 0..ix })
        }

        state.update(
            "a",
            1,
            &SegmentSpan {
                time: time.clone(),
                index: 10..20,
            },
        );
        assert_eq!(
            state.get_active_segments(),
            vec![("a".to_string(), 2)].into_iter().collect()
        );

        state.update(
            "b",
            0,
            &SegmentSpan {
                time,
                index: 10..20,
            },
        );

        assert_eq!(
            state.get_active_segments(),
            vec![("a".to_string(), 2), ("b".to_string(), 1)]
                .into_iter()
                .collect()
        );
    }
}
