import re
import os
import json
import sqlite3

from fb_sql import *

# Facebook Title patterns to match against in parse_title()
_fb_title_patterns = [
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

# List of columns in the SQLite facebook table
_db_cols = [
    'action', 'action_type', 'timestamp', 'description', 'person', 'with',
    'thread', 'title', 'url', 'fbgroup', 'camera_make', 'camera_model',
    ]

def init_db():
    """Open SQLite database, create facebook table, return connection."""
    db = sqlite3.connect('facebook.sql')
    cur = db.cursor()
    cur.execute(SQL_CREATE)
    db.commit()
    return db, cur

def parse_title(action):
    """Extract names from an action's title and update the action in place."""
    if action.get('title'):
        names = None
        # Check for matches against all defined title patterns
        for p in _fb_title_patterns:
            match = re.search(p, action['title'])
            if match:
                names = list(match.groups())
                break
        # If we got a match with two names, extract the names
        if names and len(names) == 2:
            if names[1] == 'your' or ' own ' in names[1]:
                names[1] = ME
            else:
                names[1] = names[1].split("'")[0]
            action['person'] = names[0]
            action['with'] = names[1]
        # If the title starts with our name, it's a self-post
        elif action['title'].startswith(ME):
            action['person'] = ME
            action['with'] = None
        # Otherwise, leave the action unchanged
    return action

def parse_data(action, data):
    """Parse an action's data field, return action with updated vals."""
    d = data['data'][0]
    # Post data
    if 'post' in d:
        action['action'] = 'post'
        action['description'] = d['post']
    # Comment data
    if 'comment' in d:
        action['action'] = 'comment'
        action['action_type'] = 'comment'
        if type(d['comment']) is str:
            action['description'] = d['comment']
        elif type(d['comment']) is dict:
            action['description'] = d['comment']['comment']
            action['person'] = d['comment']['author']
    return action

def parse_attachments(action, data):
    """Parse an action's attachents field, return action with updated vals."""
    att = data['attachments'][0]['data'][0]
    # External Content
    if 'external_context' in att:
        action['description'] = att['external_context'].get('name')
        action['url'] = att['external_context'].get('url')
    # Shared Page
    if 'name' in att:
        action['description'] = att['name']
    # Life event
    if 'life_event' in att:
        action['action'] = 'life_event'
        action['description'] = att['life_event']['title']
    # Media
    if 'media' in att:
        action['url'] = att['media']['uri']
        if 'media_metadata' in att['media']:
            meta = att['media']['media_metadata']['photo_metadata']
            action['camera_make'] = meta.get('camera_make')
            action['camera_model'] = meta.get('camera_model')
    # Notes
    if 'note' in att:
        action['action'] = 'note'
        action['description'] = att['note']['title']
    return action

def process_files():
    """Normalize contents of Facebook data files for easier processing.

    Goes through each Facebook data export directory / JSON file, and
    normalizes the list into a series of dicts with the following keys:
    -- action / action_type: categorization of action
    -- timestamp: Unix timestamp of action
    -- title: FB title of action
    -- person: FB user who did action
    -- with: FB user who action was done with/to (parsed from title)
    -- fbgroup: FB group action was done in
    -- description: comment / post text, album name, group name, etc.
    -- thread: messages-only, ID of messanger thread
    -- url: URL of shared link or photo
    -- camera_make / camera_model: camera metadata from photos
    Each action dict is emitted in sequence as a generator.
    General FB JSON structure is to have a single key, whose value is
    a list of actions / events.
    """

    # apps_and_websites
    os.chdir(FB_DIR + 'apps_and_websites')
    data = json.load(open('posts_from_apps_and_websites.json'))
    for row in data['app_posts']:
        r = {'action': 'app_post', 'action_type': 'post', 'person': ME,
             'timestamp': row['timestamp'], 'title': row.get('title')}
        if 'data' in row:
            r = parse_data(r, row)
        if 'attachments' in row:
            r = parse_attachments(r, row)
        yield r

    # comments
    os.chdir(FB_DIR + 'comments')
    data = json.load(open('comments.json'))
    for row in data['comments']:
        com = row['data'][0]['comment']
        yield {'action': 'comment', 'action_type': 'comment',
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
                   'timestamp': row['timestamp_ms'],
                   'person': row.get('sender_name'),
                   'thread': chat, 'description': row.get('content')}

    # photos
    os.chdir(FB_DIR + 'photos_and_videos/album')
    for album in os.listdir():
        data = json.load(open(album))
        # Photo Album
        yield {'action': 'album', 'action_type': 'post', 'person': ME,
               'timestamp': data['last_modified_timestamp'],
               'description': data['name']}

        # Album Comments
        for com in data.get('comments', list()):
            yield {'action': 'comment', 'action_type': 'comment',
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
                yield {'action': 'comment', 'action_type': 'comment',
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
            r = parse_data(r, row)
        if 'attachments' in row:
            r = parse_attachments(r, row)
        yield r

    # profile_information
    os.chdir(FB_DIR + 'profile_information')
    data = json.load(open('profile_update_history.json'))
    for row in data['profile_updates']:
        yield {'action': 'update_profile', 'action_type': 'update_profile',
               'timestamp': row['timestamp'], 'title': row.get('title')}

def insert_row(cur, data):
    """Insert a cleaned facebook action dict into SQLite's facebook table."""
    # Build INSERT statement
    q = "INSERT INTO facebook (" + ','.join(_db_cols) + ") \
         VALUES (" + ','.join(['?']*len(_db_cols)) + ");"
    # Build record to insert
    row = tuple(data.get(c) for c in _db_cols)
    cur.execute(q, row)

def _prompt_cohort(db, cur):
    cur.execute(SQL_GET_BLANK_COHORT)
    friends = cur.fetchall()
    for f in friends:
        name = f[0]
        cohort = input(name + ': ')
        cur.execute(SQL_UPDATE_COHORT, (cohort, name))
    db.commit()

if __name__ == '__main__':
    # Set paths, vars, initialize database
    FB_DIR = '/Users/harry/Desktop/Chat Data/facebook-htshore/'
    ME = 'Harrison Shore'
    os.chdir('/Users/harry/Documents/GitHub/fb_activity_graph')
    db, cur = init_db()
    
    # Load Facebook activity into database
    for i in process_files():
        insert_row(cur, parse_title(i))
    db.commit()

    # Try to estimate when removed friends were added
    cur.execute(SQL_ESTIMATE_REMOVED_FRIENDS)
    db.commit()

    # Update friend mapping table
    cur.execute(SQL_UPDATE_FRIEND_TABLE)
    db.commit()
    _prompt_cohort(db, cur)

    # Convert timestamps to dates
    cur.execute(SQL_FORMAT_DATES)
    db.commit()
