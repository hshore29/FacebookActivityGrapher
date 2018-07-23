import re
import os
import json
import sqlite3

def init_db():
    db = sqlite3.connect('facebook.sql')
    cur = db.cursor()
    return db, cur

TITLE_PATTERNS = [
    "(.+) wrote on (.+) timeline.",
    "(.+) like[ds] (.+)'s .*",
    "(.+) reacted to (.+)'s .*",
    "(.+) shared a link to (.+) timeline.",
    "(.+) shared a photo to (.+) timeline.",
    "(.+) shared an album to (.+) timeline.",
    "(.+) shared a post to (.+) timeline.",
    "(.+) posted in (.+)",
    "(.+) added a new photo to (.+) timeline.",
    "(.+) commented on (.+)'s .*",
    "(.+) replied to (.+)",
    ]

def parse_title(title):
    names = None
    for p in TITLE_PATTERNS:
        match = re.search(p, title)
        if match:
            names = list(match.groups())
            break
    if names and len(names) == 2:
        if names[1] == 'your' or ' own ' in names[1]:
            names[1] = ME
        else:
            names[1] = names[1].split("'")[0]
        return names
    else:
        return (ME, None)

def parse_data(action, data):
    d = data['data'][0]
    if 'post' in d:
        action['action'] = 'post'
        action['description'] = d['post']
    if 'comment' in d:
        action['action'] = 'comment'
        if type(d['comment']) is str:
            action['description'] = d['comment']
        elif type(d['comment']) is dict:
            action['description'] = d['comment']['comment']
            action['person'] = d['comment']['author']

def parse_attachments(action, data):
    att = data['attachments'][0]['data'][0]
    # External Content
    if 'external_context' in att:
        action['description'] = att['external_context'].get('name')
        action['url'] = att['external_context'].get('url')
        return tuple()
    # Shared Page
    if 'name' in att:
        action['description'] = att['name']
        return tuple()
    # Life event
    if 'life_event' in att:
        action['action'] = 'life_event'
        action['description'] = att['life_event']['title']
        return tuple()
    # Media
    if 'media' in att:
        action['url'] = att['media']['uri']
        if 'media_metadata' in att['media']:
            meta = att['media']['media_metadata']['photo_metadata']
            action['camera_make'] = meta.get('camera_make')
            action['camera_model'] = meta.get('camera_model')
        return tuple()

def process_files():
    # apps_and_websites
    os.chdir(FB_DIR + 'apps_and_websites')
    data = json.load(open('posts_from_apps_and_websites.json'))
    for row in data['app_posts']:
        r = {'action': 'app_post', 'action_type': 'post', 'person': ME,
             'timestamp': row['timestamp'], 'title': row.get('title')}
        if 'data' in row:
            parse_data(r, row)
        if 'attachments' in row:
            atts = parse_attachments(r, row)
            for a in atts:
                yield a
        yield r

    # comments
    os.chdir(FB_DIR + 'comments')
    data = json.load(open('comments.json'))
    for row in data['comments']:
        com = row['data'][0]['comment']
        yield {'action': 'comment', 'action_type': 'post',
               'timestamp': com['timestamp'], 'person': com['author'],
               'description': com['comment'], 'fbgroup': com.get('group'),
               'title': row['title']}

    # events
    os.chdir(FB_DIR + 'events')
    data = json.load(open('event_invitations.json'))
    for row in data['events_invited']:
        yield {'action': 'was_invited', 'action_type': 'event',
               'timestamp': row['start_timestamp'], 'description': row['name']}

    data = json.load(open('your_event_responses.json'))
    for row in data['event_responses']['events_joined']:
        yield {'action': 'accepted', 'action_type': 'event',
               'timestamp': row['start_timestamp'], 'description': row['name']}
    for row in data['event_responses']['events_declined']:
        yield {'action': 'declined', 'action_type': 'event',
               'timestamp': row['start_timestamp'], 'description': row['name']}
    for row in data['event_responses']['events_interested']:
        yield {'action': 'interested', 'action_type': 'event',
               'timestamp': row['start_timestamp'], 'description': row['name']}

    data = json.load(open('your_events.json'))
    for row in data['your_events']:
        yield {'action': 'hosting', 'action_type': 'event',
               'timestamp': row['start_timestamp'], 'description': row['name']}

    # friends
    os.chdir(FB_DIR + 'friends')
    data = json.load(open('friends.json'))
    for row in data['friends']:
        yield {'action': 'accepted', 'action_type': 'friend',
               'timestamp': row['timestamp'], 'person': row['name']}
    data = json.load(open('received_friend_requests.json'))
    for row in data['received_requests']:
        yield {'action': 'received_request', 'action_type': 'friend',
               'timestamp': row['timestamp'], 'person': row['name']}
    data = json.load(open('rejected_friend_requests.json'))
    for row in data['rejected_requests']:
        yield {'action': 'rejected', 'action_type': 'friend',
               'timestamp': row['timestamp'], 'person': row['name']}
    data = json.load(open('removed_friends.json'))
    for row in data['deleted_friends']:
        yield {'action': 'removed', 'action_type': 'friend',
               'timestamp': row['timestamp'], 'person': row['name']}
    data = json.load(open('sent_friend_requests.json'))
    for row in data['sent_requests']:
        yield {'action': 'sent_request', 'action_type': 'friend',
               'timestamp': row['timestamp'], 'person': row['name']}

    # groups
    os.chdir(FB_DIR + 'groups')
    data = json.load(open('your_groups.json'))
    for row in data['groups_admined']:
        yield {'action': 'group_admined', 'action_type': 'group_admined',
               'timestamp': row['timestamp'], 'description': row['name']}

    # likes_and_reactions
    os.chdir(FB_DIR + 'likes_and_reactions')
    data = json.load(open('pages.json'))
    for row in data['page_likes']:
        yield {'action': 'like_page', 'action_type': 'like',
               'timestamp': row['timestamp'], 'title': row.get('title'),
               'description': row['data'][0]['name']}

    data = json.load(open('posts_and_comments.json'))
    for row in data['reactions']:
        react = row['data'][0]['reaction']
        yield {'action': react['reaction'], 'action_type': 'like',
               'timestamp': row['timestamp'], 'title': row.get('title'),
               'person': react['actor']}

    # messages
    os.chdir(FB_DIR + 'messages')
    for chat in os.listdir():
        if chat == 'stickers_used':
            continue
        data = json.load(open(chat + '/message.json'))
        for row in data['messages']:
            yield {'action': 'message', 'action_type': 'message',
                   'timestamp': row['timestamp'], 'person': row['sender_name'],
                   'thread': chat, 'description': row.get('content')}

    # photos
    os.chdir(FB_DIR + 'photos/album')
    for album in os.listdir():
        data = json.load(open(album))
        # Photo Album
        yield {'action': 'album', 'action_type': 'post', 'person': ME,
               'timestamp': data['last_modified_timestamp'],
               'description': data['name']}

        # Album Comments
        for com in data.get('comments', list()):
            yield {'action': 'comment', 'action_type': 'post',
                   'timestamp': com['timestamp'], 'person': com['author'],
                   'description': com['comment'], 'fbgroup': com.get('group')}

        # Photos
        for photo in data['photos']:
            if 'creation_timestamp' in photo:
                ts = photo['creation_timestamp']
            else:
                if 'comments' in photo:
                    ts = min(c['timestamp'] for c in photo['comments'])
                else:
                    ts = None
            r = {'action': 'album_photo', 'action_type': 'post',
                 'timestamp': ts, 'url': photo['uri'], 'person': ME,
                 'description': data['name']}
            if 'media_metadata' in photo:
                meta = photo['media_metadata']['photo_metadata']
                r['camera_make'] = meta.get('camera_make')
                r['camera_model'] = meta.get('camera_model')
            yield r

            # Photo Comments
            for com in photo.get('comments', list()):
                if com['author'] == ME:
                    continue
                yield {'action': 'comment', 'action_type': 'post',
                       'timestamp': com['timestamp'],
                       'person': com['author'], 'description': com['comment'],
                       'fbgroup': com.get('group')}

    # posts
    os.chdir(FB_DIR + 'posts')
    data1 = json.load(open('your_posts.json'))
    data2 = json.load(open("other_people's_posts_to_your_timeline.json"))
    posts = data1['status_updates'] + data2['wall_posts_sent_to_you']
    for row in posts:
        r = {'action': 'post', 'action_type': 'post',
             'timestamp': row['timestamp'], 'title': row.get('title')}
        if 'data' in row:
            parse_data(r, row)
        if 'attachments' in row:
            atts = parse_attachments(r, row)
            for a in atts:
                yield a
        yield r

    # profile_information
    os.chdir(FB_DIR + 'profile_information')
    data = json.load(open('profile_update_history.json'))
    for row in data['profile_updates']:
        yield {'action': 'update_profile', 'action_type': 'update_profile',
               'timestamp': row['timestamp'], 'title': row['title']}

def clean_title(action):
    if action.get('title'):
        from_to = parse_title(action['title'])
        action['person'] = from_to[0]
        action['with'] = from_to[1]
    return action

def insert_row(cur, data):
    cols = ['action', 'action_type', 'timestamp', 'description', 'person',
            'with', 'thread', 'title', 'url', 'fbgroup', 'camera_make',
            'camera_model']
    q = 'insert into facebook (' + ','.join(cols) + ') values ('
    q += ','.join(['?']*len(cols)) + ');'
    row = tuple(data.get(c) for c in cols)
    cur.execute(q, row)

def prompt_cohort(db, cur):
    cur.execute('SELECT person FROM friends WHERE cohort IS NULL')
    friends = cur.fetchall()
    for f in friends:
        name = f[0]
        cohort = input(name + ': ')
        cur.execute('UPDATE friends SET cohort=? WHERE person=?', (cohort, name))
    db.commit()

if __name__ == '__main__':
    # Set paths, vars, initialize database
    FB_DIR = '/Users/harry/Desktop/Chat Data/facebook-htshore/'
    ME = 'Harrison Shore'
    os.chdir('/Users/harry/Documents/GitHub/fb_activity_graph')
    db, cur = init_db()
    
    # Load Facebook activity into database
    for i in process_files():
        insert_row(cur, clean_title(i))
    db.commit()

    # Try to estimate when removed friends were added
    cur.execute("""
INSERT INTO facebook (action, action_type, person, timestamp)
SELECT 'accepted_est', 'friend', f.person, (
  SELECT min(timestamp) FROM facebook
  WHERE action != 'removed' AND (person = f.person OR with = f.person)
    AND timestamp <= f.timestamp) AS est_date
FROM facebook f
WHERE action_type = 'friend' AND action = 'removed' AND est_date is not null
""")
    db.commit()
    cur.execute("""
INSERT INTO friends (person)
SELECT person FROM facebook WHERE action in ('accepted', 'accepted_est')
  AND action_type = 'friend' AND person NOT IN (SELECT person FROM friends)
""")
    db.commit()
    populate_cohorts(db, cur)

    # Convert timestamps to dates
    cur.execute("""UPDATE facebook SET fb_date=datetime(timestamp, 'unixepoch')
                   WHERE timestamp IS NOT NULL""")
    db.commit()
