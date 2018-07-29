import sqlite3
from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

### Get Data
db = sqlite3.connect('facebook.sql')
cur = db.cursor()

def group_by(data, group_index):
    dates = set()
    grouped = defaultdict(lambda: defaultdict(int))
    for row in data:
        dates.add(row[0])
        grouped[row[group_index]][row[0]] += row[-1]
    dates = sorted(dates)
    dataframe = {'date': dates}
    for key, vals in grouped.items():
        dataframe[key] = np.array([vals[d] for d in dates])
    return dataframe

cur.execute("""
SELECT
  cast(strftime("%Y", fb_date) AS FLOAT) +
    ((cast(strftime("%m", fb_date) AS INT) - 1) / 3) / 4.0 AS quarter,
  CASE WHEN person = 'Harrison Shore' THEN
      CASE WHEN with IS NULL THEN 'self' ELSE 'me' END
  ELSE 'other' END AS person1,
  action_type, count(*)
FROM facebook WHERE action != "album_photo" AND fb_date IS NOT NULL
GROUP BY person1, quarter, action_type;""")
data = list(cur.fetchall())
actions = group_by(data, group_index=2)
excl = ['date', 'friend', 'update_profile', 'group_admined', 'message']
actions['total'] = sum([v for k, v in actions.items() if k not in excl])
data = [d for d in data if d[2] == 'post']
post_balance = group_by(data, group_index=1)

cur.execute("""
SELECT
  cast(strftime("%Y", fb_date) AS FLOAT) +
    ((cast(strftime("%m", fb_date) AS INT) - 1) / 3) / 4.0 AS quarter,
  cohort,
  sum(CASE WHEN action LIKE 'accepted%' THEN 1 ELSE -1 END)
FROM facebook f JOIN friends d ON f.person = d.person
WHERE action_type = 'friend'
  AND action IN ('accepted', 'removed', 'accepted_est')
GROUP BY quarter, cohort;""")
friends = group_by(cur.fetchall(), group_index=1)
for k, v in friends.items():
    friends[k] = np.cumsum(v) if k != 'date' else v
friends['total'] = sum([v for k, v in friends.items() if k != 'date'])

# Draw globals
colors = {
    'post': '#4260B4',
    'comment': '#8ED66F',
    'event': '#9B0024',
    'friend': '#D2D5DA',
    'like': '#5885FF',
    'message': '#008BFF',
    }

### First Plot
def init_axis(data, ytick, figsize):
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_axisbelow(True)
    ax.yaxis.grid()
    ax.yaxis.set_tick_params(left=False, labelleft=False)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(ytick))
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.set_xlim([min(data['date']), max(data['date'])])
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    plt.tight_layout()
    return fig, ax

def plot_1(data):
    # Posts v. Likes
    fig, ax = init_axis(data, 100, (14, 7))
    ax.stackplot(data['date'], data['post'] + data['comment'] + data['event'],
                 data['like'], labels=['Posts', 'Likes'],
                 colors=[colors['post'], colors['like']])
    fig.savefig('figure_5.png')

def plot_2(data):
    # Percent Likes
    fig, ax = init_axis(data, 0.25, (7, 5))
    ax.fill_between(data['date'], data['like'] / data['total'],
                    color=colors['like'])
    ax.set_ylim([0, 1])
    fig.savefig('figure_6.png')

def plot_3(data):
    # Basic friend count
    fig, ax = init_axis(data, 100, (7, 5))
    ax.fill_between(data['date'], data['total'], color=colors['post'])
    ax.set_ylim([0, data['total'].max()])
    fig.savefig('figure_7.png')

def plot_4(data):
    # Grouped friend chart
    fig, ax = init_axis(data, 100, (7, 5))
    ax.stackplot(data['date'], data['Ballston Spa'], data['Cornell'],
                 data['Family'], data['Mindshare'],
                 colors=[colors['like'], colors['comment'],
                         colors['post'], colors['event']])
    fig.savefig('figure_8.png')

def plot_5(data):
    # Post balance
    fig, ax = init_axis(data, 50, (7, 5))
    ax.fill_between(data['date'], data['other']*-1, color=colors['comment'])
    ax.stackplot(data['date'], data['me'], data['self'],
                 colors=[colors['like'], colors['post']])
    fig.savefig('figure_9.png')
