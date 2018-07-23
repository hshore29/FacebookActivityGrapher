import sqlite3
db = sqlite3.connect('facebook.sql')
cur = db.cursor()

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from datetime import datetime

### Get Data
# Post Type data
cur.execute("""
select
  --substr(date(fb_date), 0, 8)
  cast(strftime("%Y", fb_date) as FLOAT) +
  ((cast(strftime("%m", fb_date) as INT) - 1) / 3) / 4.0 as quarter,
  sum(case when action_type = 'like' then 1 else 0 end),
  sum(case when action_type = 'event' then 1 else 0 end),
  sum(case when action_type = 'friend' then 1 else 0 end),
  sum(case when action_type = 'post' and action = 'comment' then 1 else 0 end),
  sum(case when action_type = 'post' and action != 'comment' then 1 else 0 end),
  count(*)
from facebook
where action not in ("album_photo", "message") and fb_date is not null
group by quarter""")
data = np.array(cur.fetchall(), dtype=[
    ('date', 'float'),
    ('like', 'i4'),
    ('event', 'i4'),
    ('friend', 'i4'),
    ('comment', 'i4'),
    ('post', 'i4'),
    ('total', 'i4'),
    ])

# Friend count data
cur.execute("""
select
  cast(strftime("%Y", fb_date) as FLOAT) +
  ((cast(strftime("%m", fb_date) as INT) - 1) / 3) / 4.0 as quarter,
  --sum(case when action like 'accepted%' then 1 else -1 end)
  sum(case when cohort = 'Ballston Spa' then
    case when action like 'accepted%' then 1 else -1 end
  else 0 end),
  sum(case when cohort = 'Cornell' then
    case when action like 'accepted%' then 1 else -1 end
  else 0 end),
  sum(case when cohort = 'Family' then
    case when action like 'accepted%' then 1 else -1 end
  else 0 end),
  sum(case when cohort = 'Mindshare' then
    case when action like 'accepted%' then 1 else -1 end
  else 0 end)
from facebook f
join friends d on f.person = d.person
where action_type = "friend"
and action in ('accepted', 'removed', 'accepted_est')
group by quarter
""")
f_data = np.array(cur.fetchall(), dtype=[
    ('date', 'float'),
    #('friends', 'i4'),
    ('highschool', 'i4'),
    ('college', 'i4'),
    ('family', 'i4'),
    ('firstjob', 'i4'),
    ])

# Wall Posts me vs. others
cur.execute("""
select
  cast(strftime("%Y", fb_date) as FLOAT) +
  ((cast(strftime("%m", fb_date) as INT) - 1) / 3) / 4.0 as quarter,
  sum(case when person = 'Harrison Shore' and with is null then 1 else 0 end),
  sum(case when person = 'Harrison Shore' and with is not null then 1 else 0 end),
  sum(case when person != 'Harrison Shore' then 1 else 0 end)
from facebook f
where person is not null and action = 'post'
group by quarter
""")
w_data = np.array(cur.fetchall(), dtype=[
    ('date', 'float'),
    ('self', 'i4'),
    ('me', 'i4'),
    ('friend', 'i4'),
    ])

# Draw globals
resolution = 1/4
colors = {
    'post': '#4260B4',
    'comment': '#8ED66F',
    'event': '#9B0024',
    'friend': '#D2D5DA',
    'like': '#5885FF',
    'message': '#008BFF',
    }

### First Plot
def init_axis(plt, ax, data):
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='grey', linestyle='-')
    ax.yaxis.set_tick_params(direction='out', left=False, right=False,
                             labelleft=False)
    ax.xaxis.set_tick_params(direction='out', top=False)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.set_xlim([data['date'].min(), data['date'].max()])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    plt.tight_layout()

def plot_1():
    # Posts v. Likes
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.stackplot(data['date'], data['post'] + data['comment'] + data['event'],
                 data['like'], labels=['Posts', 'Likes'], edgecolor='none',
                 colors=[colors['post'], colors['like']])
    init_axis(plt, ax, data)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(100))
    #ax.set_title('Facebook activity by quarter')

    #plt.legend(loc='upper left')
    fig.savefig('figure_5.png')
    plt.show()

def plot_2():
    # Percent Likes
    fig, ax = plt.subplots(figsize=(7, 5))
    init_axis(plt, ax, data)
    ax.fill_between(data['date'], data['like'] / data['total'],
                    color=colors['like'])
    ax.set_ylim([0, 1])
    ax.yaxis.set_major_locator(ticker.MultipleLocator(0.25))
    fig.savefig('figure_6.png')
    plt.show()

def plot_3():
    # Basic friend count
    fig, ax = plt.subplots(figsize=(7, 5))
    init_axis(plt, ax, f_data)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(100))
    ax.fill_between(f_data['date'], np.cumsum(f_data['friends']),
                    color=colors['post'])
    fig.savefig('figure_7.png')
    plt.show()

def plot_4():
    # Grouped friend chart
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.stackplot(f_data['date'],
                 np.cumsum(f_data['highschool']),
                 np.cumsum(f_data['college']),
                 np.cumsum(f_data['family']),
                 np.cumsum(f_data['firstjob']),
                 edgecolor='none',
                 colors=[colors['like'], colors['comment'],
                         colors['post'], colors['event']])
    init_axis(plt, ax, f_data)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(100))
    fig.savefig('figure_8.png')
    plt.show()

fig, ax = plt.subplots(figsize=(7, 5))
ax.fill_between(w_data['date'], w_data['friend']*-1, color=colors['comment'])
ax.stackplot(w_data['date'],
             w_data['me'],
             w_data['self'],
             edgecolor='none',
             colors=[colors['like'], colors['post']])
init_axis(plt, ax, w_data)
fig.savefig('figure_9.png')
plt.show()
