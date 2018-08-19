SQL_CREATE = """
CREATE TABLE IF NOT EXISTS facebook (
  action text, action_type text, timestamp int, description text,
  person text, thread text, title text, url text, fbgroup text,
  camera_make text, camera_model text, fb_date datetime, with text
);"""

SQL_CHECK = "SELECT count(*) FROM facebook;"

SQL_DELETE = "DELETE FROM facebook;"

SQL_ESTIMATE_REMOVED_FRIENDS = """
INSERT INTO facebook (action, action_type, person, timestamp)
SELECT 'accepted_est', 'friend', f.person, (
  SELECT min(timestamp) FROM facebook
  WHERE action != 'removed' AND (person = f.person OR with = f.person)
    AND timestamp <= f.timestamp) AS est_date
FROM facebook f
WHERE action_type = 'friend' AND action = 'removed'
  AND est_date is not null;
"""

SQL_UPDATE_FRIEND_TABLE = """
INSERT INTO friends (person)
SELECT person FROM facebook WHERE action in ('accepted', 'accepted_est')
  AND action_type = 'friend' AND person NOT IN (SELECT person FROM friends);
"""

SQL_FORMAT_DATES = """
UPDATE facebook SET fb_date=datetime(timestamp, 'unixepoch')
  WHERE timestamp IS NOT NULL;
"""

SQL_UPDATE_COHORT = "UPDATE friends SET cohort=? WHERE person=?;"
SQL_GET_BLANK_COHORT = "SELECT person FROM friends WHERE cohort IS NULL;"

SQL_GET_ACTION_DATA = """
SELECT
  cast(strftime("%%Y", fb_date) AS FLOAT) +
    (cast(strftime("%%m", fb_date) AS FLOAT) - 1) / 12 AS month,
  --  ((cast(strftime("%%m", fb_date) AS INT) - 1) / 3) / 4.0 AS quarter,
  CASE WHEN person = '%s' THEN
      CASE WHEN with IS NULL THEN 'self' ELSE 'me' END
  ELSE 'other' END AS person1,
  action_type, count(*)
FROM facebook WHERE action not in ('album_photo', 'message')
  AND fb_date IS NOT NULL
GROUP BY month, person1, action_type;"""

SQL_GET_FRIEND_DATA = """
SELECT
  cast(strftime("%Y", fb_date) AS FLOAT) +
    (cast(strftime("%m", fb_date) AS FLOAT) - 1) / 12 AS month,
  cohort,
  sum(CASE WHEN action LIKE 'accepted%' THEN 1 ELSE -1 END)
FROM facebook f JOIN friends d ON f.person = d.person
WHERE action_type = 'friend'
  AND action IN ('accepted', 'removed', 'accepted_est')
GROUP BY month, cohort;"""
