use crate::topic::SegmentData;
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
                        index_end       INTEGER,
                        size            INTEGER
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
        // TODO clear pending segments (size=NULL)
        TopicState { path, connection }
    }

    pub(crate) fn update(
        &self,
        partition: &str,
        segment_ix: usize,
        data: &SegmentData<Range<u128>>,
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
                index_end,
                size
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(partition, segment_id) DO UPDATE SET
                time_start=excluded.time_start;
                time_end=excluded.time_end;
                index_start=excluded.index_start;
                index_end=excluded.index_end;
        ",
                params![
                    partition,
                    segment_ix,
                    OffsetDateTime::from(*data.time.start()),
                    OffsetDateTime::from(*data.time.end()),
                    u64::try_from(data.index.start).unwrap(),
                    u64::try_from(data.index.end).unwrap(),
                    data.size
                ],
            )
            .unwrap();
    }

    pub(crate) fn update_size(&self, partition: &str, segment_ix: usize, size: u64) -> () {
        self.connection
            .execute(
                "
            UPDATE segments
            SET size = ?1
            WHERE partition = ?2 AND segment_id = ?3
        ",
                params![size, partition, segment_ix],
            )
            .unwrap();
    }

    pub fn get_segment_span(
        &self,
        partition: &str,
        segment_ix: usize,
    ) -> Option<SegmentData<Range<u128>>> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT time_start, time_end, index_start, index_end, size FROM segments
            WHERE partition = ?1 AND segment_id = ?2
        ",
            )
            .unwrap();

        let r = statement
            .query_map(params![partition, segment_ix], |row| {
                Ok(SegmentData {
                    time: (SystemTime::from(row.get::<_, OffsetDateTime>(0)?)
                        ..=SystemTime::from(row.get::<_, OffsetDateTime>(1)?)),
                    index: (u128::from(row.get::<_, u64>(2)?)..u128::from(row.get::<_, u64>(3)?)),
                    size: row.get(4)?,
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

    pub fn get_min_segment(&self, partition: &str) -> Option<usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT segment_id FROM segments
            WHERE partition = ?1
            ORDER BY segment_id ASC LIMIT 1
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

    pub fn remove_segment(&self, partition: &str, segment_ix: usize) {
        self.connection
            .execute(
                "
            DELETE FROM segments
            WHERE partition = ?1 AND segment_id = ?2
        ",
                params![partition, segment_ix],
            )
            .unwrap();
    }

    pub fn get_size(&self, partition: &str) -> Option<u64> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT SUM(size) AS size FROM segments
            WHERE partition = ?1
            GROUP BY partition
        ",
            )
            .unwrap();

        let r = statement
            .query_map([partition], |row| Ok(row.get(0)?))
            .unwrap()
            .next();

        r.and_then(|v: Result<Option<u64>>| v.unwrap())
    }

    pub fn open_index(&self, name: &str) -> u128 {
        self.get_active_segment(name)
            .and_then(|segment_ix| {
                self.get_segment_span(name, segment_ix - 1)
                    .map(|span| span.index.end)
            })
            .unwrap_or(0)
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
        assert_eq!(state.get_active_segment(""), None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update(
                "",
                0,
                &SegmentData {
                    time,
                    index: 0..ix,
                    size: Some(10),
                },
            )
        }

        state.update(
            "",
            1,
            &SegmentData {
                time,
                index: 10..20,
                size: Some(15),
            },
        );
        assert_eq!(state.get_segment_for_ix("", 0), Some(0));
        assert_eq!(state.get_segment_for_ix("", 5), Some(0));
        assert_eq!(state.get_segment_for_ix("", 9), Some(0));
        assert_eq!(state.get_segment_for_ix("", 15), Some(1));
        assert_eq!(state.get_segment_for_ix("", 200), Some(1));
        assert_eq!(state.get_active_segment(""), Some(2));
    }

    #[test]
    fn test_partitions() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = TopicState::attach(PathBuf::from(path));
        assert_eq!(state.get_active_segments(), HashMap::new());
        let a = "a";
        let b = "b";

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update(
                a,
                0,
                &SegmentData {
                    time,
                    index: 0..ix,
                    size: Some(10),
                },
            )
        }

        state.update(
            a,
            1,
            &SegmentData {
                time: time.clone(),
                index: 10..20,
                size: Some(25),
            },
        );
        assert_eq!(
            state.get_active_segments(),
            vec![(a.to_string(), 2)].into_iter().collect()
        );

        state.update(
            b,
            0,
            &SegmentData {
                time,
                index: 0..20,
                size: Some(12),
            },
        );

        assert_eq!(
            state.get_active_segments(),
            vec![(a.to_string(), 2), (b.to_string(), 1)]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_remove() {
        let root = tempdir().unwrap();
        let path = root.path().join("testing.sqlite");
        let state = TopicState::attach(PathBuf::from(path));
        assert_eq!(state.get_active_segment(""), None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update(
                "",
                0,
                &SegmentData {
                    time,
                    index: 0..ix,
                    size: None,
                },
            )
        }
        assert_eq!(state.get_size(""), None);
        state.update_size("", 0, 12);
        assert_eq!(state.get_size(""), Some(12));

        state.update(
            "",
            1,
            &SegmentData {
                time,
                index: 10..20,
                size: None,
            },
        );
        state.update_size("", 1, 13);
        assert_eq!(state.get_size(""), Some(25));

        assert_eq!(state.get_segment_for_ix("", 0), Some(0));
        assert_eq!(state.get_segment_for_ix("", 15), Some(1));
        assert_eq!(state.get_min_segment(""), Some(0));

        state.remove_segment("", 0);
        assert_eq!(state.get_segment_for_ix("", 0), None);
        assert_eq!(state.get_segment_for_ix("", 15), Some(1));
        assert_eq!(state.get_min_segment(""), Some(1));
        assert_eq!(state.get_size(""), Some(13));

        state.remove_segment("", 1);
        assert_eq!(state.get_segment_for_ix("", 0), None);
        assert_eq!(state.get_segment_for_ix("", 15), None);
        assert_eq!(state.get_min_segment(""), None);
        assert_eq!(state.get_size(""), None);
    }
}
