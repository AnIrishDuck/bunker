use crate::topic::SegmentSpan;
use rusqlite::{params, Connection, Result};
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
                    "CREATE UNIQUE INDEX segments_segment_id ON segments(segment_id)",
                    [],
                )
                .unwrap();
            connection
                .execute("CREATE INDEX segments_start ON segments(index_start)", [])
                .unwrap();
        }
        TopicState { path, connection }
    }

    pub(crate) fn update(&self, segment_ix: usize, ixs: &SegmentSpan<Range<u128>>) -> () {
        self.connection.execute("
            INSERT INTO segments(segment_id, time_start, time_end, index_start, index_end) VALUES(?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(segment_id) DO UPDATE SET
                time_start=excluded.time_start;
                time_end=excluded.time_end;
                index_start=excluded.index_start;
                index_end=excluded.index_end;
        ", params![
            segment_ix,
            OffsetDateTime::from(*ixs.time.start()),
            OffsetDateTime::from(*ixs.time.end()),
            u64::try_from(ixs.index.start).unwrap(),
            u64::try_from(ixs.index.end).unwrap()
        ]).unwrap();
    }

    pub fn get_segment_span(&self, segment_ix: usize) -> Option<SegmentSpan<Range<u128>>> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT time_start, time_end, index_start, index_end FROM segments
            WHERE segment_id = ?1
        ",
            )
            .unwrap();

        let r = statement
            .query_map(params![segment_ix], |row| {
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

    pub fn get_segment_for_ix(&self, ix: u64) -> Option<usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT segment_id FROM segments
            WHERE index_start <= ?1 ORDER BY segment_id DESC LIMIT 1
        ",
            )
            .unwrap();

        let r = statement
            .query_map(params![ix], |row| row.get(0))
            .unwrap()
            .next();

        r.map(|v| v.unwrap())
    }

    pub fn get_active_segment(&self) -> Option<usize> {
        let mut statement = self
            .connection
            .prepare(
                "
            SELECT segment_id FROM segments
            ORDER BY segment_id DESC LIMIT 1
        ",
            )
            .unwrap();

        let r = statement.query_map([], |row| row.get(0)).unwrap().next();

        r.map(|v: Result<usize, _>| v.unwrap() + 1)
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
        assert_eq!(state.get_active_segment(), None);

        let time = SystemTime::UNIX_EPOCH..=SystemTime::UNIX_EPOCH;
        for ix in 0..10 {
            let time = time.clone();
            state.update(0, &SegmentSpan { time, index: 0..ix })
        }

        state.update(
            1,
            &SegmentSpan {
                time,
                index: 10..20,
            },
        );
        assert_eq!(state.get_segment_for_ix(0), Some(0));
        assert_eq!(state.get_segment_for_ix(5), Some(0));
        assert_eq!(state.get_segment_for_ix(9), Some(0));
        assert_eq!(state.get_segment_for_ix(15), Some(1));
        assert_eq!(state.get_segment_for_ix(200), Some(1));
        assert_eq!(state.get_active_segment(), Some(2));
    }
}
