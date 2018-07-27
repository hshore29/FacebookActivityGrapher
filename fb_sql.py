SQL_CREATE = """
CREATE TABLE IF NOT EXISTS facebook (
  action text, action_type text, timestamp int, description text,
  person text, thread text, title text, url text, fbgroup text,
  camera_make text, camera_model text, fb_date datetime, with text
);"""

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
