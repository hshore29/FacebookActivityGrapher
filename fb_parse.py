import re
import os
import json
import sqlite3
from collections import defaultdict

import numpy as np
from matplotlib import rcParams
rcParams['font.sans-serif'] = ['Helvetica', 'Arial', 'sans-serif']
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

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

# Facebook-style colors for drawing charts
_colors = {
    'post': '#4260B4',
    'dkblue': '#192648',
    'comment': '#8ED66F',
    'event': '#9B0024',
    'friend': '#D2D5DA',
    'like': '#5885FF',
    'message': '#008BFF',
    'badge': '#FF0017',
    }

def init_db():
    """Open SQLite database, create facebook table, return connection."""
    db = sqlite3.connect('facebook.sql')
    cur = db.cursor()
    cur.execute(SQL_CREATE)
    db.commit()
    cur.execute(SQL_CHECK)
    parse = list(cur.fetchall())[0][0] == 0
    return db, cur, parse

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
    os.chdir(base_dir + 'apps_and_websites')
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
    os.chdir(base_dir + 'comments')
    data = json.load(open('comments.json'))
    for row in data['comments']:
        com = row['data'][0]['comment']
        yield {'action': 'comment', 'action_type': 'comment',
               'timestamp': com['timestamp'], 'person': com['author'],
               'description': com['comment'], 'fbgroup': com.get('group'),
               'title': row['title']}

    # events
    os.chdir(base_dir + 'events')
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
    os.chdir(base_dir + 'friends')
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
    os.chdir(base_dir + 'groups')
    data = json.load(open('your_groups.json'))
    for row in data['groups_admined']:
        yield {'action': 'group_admined', 'action_type': 'group_admined',
               'timestamp': row['timestamp'], 'description': row['name']}

    # likes_and_reactions
    os.chdir(base_dir + 'likes_and_reactions')
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
    os.chdir(base_dir + 'messages')
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
    os.chdir(base_dir + 'photos_and_videos/album')
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
    os.chdir(base_dir + 'posts')
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
    os.chdir(base_dir + 'profile_information')
    data = json.load(open('profile_update_history.json'))
    for row in data['profile_updates']:
        r = {'action': 'update_profile', 'action_type': 'update_profile',
             'timestamp': row['timestamp'], 'title': row.get('title')}
        if 'attachments' in row:
            r = parse_attachments(r, row)
        yield r

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
    if friends:
        print("Group friends by how you met them")
        print("i.e. High School, Family, College, First Job, etc.")
        print("Each friend will be prompted, type the same group for")
        print("all friends that should be grouped together.")
    for f in friends:
        name = f[0]
        cohort = input(name + ': ')
        cur.execute(SQL_UPDATE_COHORT, (cohort, name))
    db.commit()

def group_by(data, group_index):
    """Group a list of lists by the data in position i. Returns array dict."""
    dates = set()
    grouped = defaultdict(lambda: defaultdict(int))
    # Group data in dictionary
    for row in data:
        dates.add(row[0])
        grouped[row[group_index]][row[0]] += row[-1]
    # Sort dates, initialize "dataframe"
    dates = sorted(dates)
    dataframe = {'date': dates}
    # Loop over dates, convert data dicts to numpy arrays
    for key, vals in grouped.items():
        dataframe[key] = np.array([vals[d] for d in dates])
    return dataframe

def get_data(cur, query):
    """Fetch data from database."""
    cur.execute(query)
    return list(cur.fetchall())

def draw_chart(file_name, plotter, data, figsize, pct=False):
    """Draw a figure using plotter function, save it as a png."""
    # Initialize figure
    fig, ax = plt.subplots(figsize=figsize)
    # Set axis & tick parameters
    ax.set_axisbelow(True)
    ax.yaxis.grid()
    ax.yaxis.set_tick_params(left=False, labelleft=True)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(50))
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    # Override x-limits
    ax.set_xlim([min(data['date']), max(data['date'])])
    # If this is a % chart, set the tick and ylimits accordingly
    if pct:
        ax.yaxis.set_major_locator(ticker.MultipleLocator(0.25))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter('{0:.0%}'.format))
        ax.set_ylim([0, 1])
    # If this uses negatives to show other's posts, format away the minus
    if file_name in ('wall_posts', 'messages'):
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(
            lambda y, _: '{:,.0f}'.format(abs(y))
            ))
    # Call plotting function
    plotter(ax, data)
    # Draw PNG
    plt.tight_layout()
    fig.savefig(file_name + '.png')

def add_title(ax, t):
    ax.set_title(t, fontdict={'fontsize': 18}, loc='left')

### Specific chart functions ###
def posts_v_likes(ax, data):
    """Draw stacked area chart of posts vs. likes."""
    add_title(ax, 'Timeline of Facebook activity')
    ax.set_ylabel('Actions per month')
    ax.stackplot(data['date'], data['post'] + data['comment'] + data['event'],
                 data['like'], labels=['Posts/Comments', 'Likes'],
                 colors=[_colors['post'], _colors['like']],
                 edgecolor='none')
    ax.legend(loc='upper center')

def pct_likes(ax, data):
    """Draw area chart of % likes."""
    add_title(ax, 'Likes as % of activity, by month')
    ax.fill_between(data['date'], data['like'] / data['total'],
                    color=_colors['like'])

def friend_count(ax, data):
    """Draw area chart of friend count."""
    add_title(ax, 'Friends over time')
    ax.fill_between(data['date'], data['total'], color=_colors['post'])
    ax.set_ylim([0, data['total'].max()])

def grouped_friend_count(ax, data):
    """Draw stacked area chart of friend cohort counts."""
    add_title(ax, 'Friend groups over time')
    labs = [k for k in data.keys() if k not in ('date', 'total')]
    ydat = [data[k] for k in labs]
    ax.stackplot(data['date'], ydat, labels=labs)
    ax.legend(loc='upper left')

def post_balance(ax, data):
    """Draw stacked area chart of post balance (me vs. others)."""
    add_title(ax, 'Balance of Wall Posts, by month')
    ax.fill_between(data['date'], data['other']*-1, color=_colors['comment'],
                    label='Posts on my wall')
    ax.stackplot(data['date'], data['me'], data['self'],
                 colors=[_colors['like'], _colors['post']],
                 labels=["My posts on other's walls",
                         "My status updates / posts"])
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc='upper center')

def messages(ax, data):
    """Draw stacked area chart of messages (sent vs. recieved)."""
    add_title(ax, 'Facebook Messages (sent vs. recieved)')
    ax.fill_between(data['date'], data['other']*-1, color=_colors['message'],
                    label='Messages received')
    ax.fill_between(data['date'], data['self'], color=_colors['message'],
                    label='Messages sent')
    ax.yaxis.set_major_locator(ticker.MultipleLocator(500))
    ax.legend()

if __name__ == '__main__':
    # Set paths, vars, initialize database
    print('Enter your name as it appears on Facebook')
    ME = input('Name: ')
    PARSE = False
    base_dir = os.getcwd()
    db, cur, parse = init_db()

    if parse:
        print('Processing Facebook activity data')
        # Load Facebook activity into database
        for i in process_files():
            insert_row(cur, parse_title(i))
        db.commit()
        os.chdir(base_dir)

        # Try to estimate when removed friends were added
        cur.execute(SQL_ESTIMATE_REMOVED_FRIENDS)
        db.commit()

        # Update friend mapping table
        cur.execute(SQL_UPDATE_FRIEND_TABLE)
        db.commit()
        _prompt_cohort(db, cur)

        # Convert timestamps to dates
        cur.execute(SQL_FORMAT_DATES)
        cur.execute(SQL_FORMAT_DATES_2)
        db.commit()

    # Create graphs folder if necessary
    os.chdir(base_dir)
    if 'graphs' not in os.listdir():
        os.mkdir('graphs')
    os.chdir('graphs')
    print('Drawing charts of Facebook activity data')

    # Post vs. Likes Chart
    data = get_data(cur, SQL_GET_ACTION_DATA % ME)
    actions = group_by(data, group_index=2)
    draw_chart('timeline', posts_v_likes, actions, (7, 5))

    # Percent Likes Chart
    excl = ['date', 'friend', 'message']
    actions['total'] = sum([v for k, v in actions.items() if k not in excl])
    draw_chart('percent_likes', pct_likes, actions, (7, 5), pct=True)

    # Messages Chart
    msg_data = [d for d in data if d[2] == 'message']
    msgs = group_by(msg_data, group_index=1)
    draw_chart('messages', messages, msgs, (7, 5))

    # Post Balance Chart
    data = [d for d in data if d[2] == 'post']
    posts = group_by(data, group_index=1)
    draw_chart('wall_posts', post_balance, posts, (7, 5))

    # Friend Charts
    data = get_data(cur, SQL_GET_FRIEND_DATA)
    friends = group_by(data, group_index=1)
    for k, v in friends.items():
        friends[k] = np.cumsum(v) if k != 'date' else v
    friends['total'] = sum([v for k, v in friends.items() if k != 'date'])
    draw_chart('friends', friend_count, friends, (7, 5))
    draw_chart('friends_cat', grouped_friend_count, friends, (7, 5))
