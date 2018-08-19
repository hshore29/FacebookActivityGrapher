# Facebook Activity Grapher
A script to parse Facebook's personal data export and generate a few descriptive charts.
## How it works
Facebook's data export consists of a dozen JSON files in different folders based on what type of activity they track. The format for each is similar - an object with one key, which has a list of objects with information about the activity.

Activity Grapher's first step is to open each of these JSON files, get the list of activities, and map them to a common set of keys. These are yield to a parent function, which inserts them into a SQLite database. SQLite is just used as an intermediary - the event log could easily be written to a CSV, or probably kept in memory, but having a simple database makes adding charts easier.

Next, a few cleanup scripts are run - converting timestamps to dates, guessing when removed friends may have first been added, and mapping friends into groups.

Finally, we use matplotlib to create a handful of explanatory graphs:
1. Timeline of actions by month
2. % of actions that are Likes by month
3. Balance of posts on your wall vs. posts on friends walls
4. Total friends over time
5. Total friends over time by group (provided during mapping)
## Downloading your data from Facebook
To get your data from Facebook, follow these steps:
1. On Facebook go to **Settings**
2. Click on **Your Facebook Information**, then **Download Your Information**
3. Click **Create File**. Facebook will let you know when it's ready, then you can download and unzip.
## Running Activity Grapher
Download fb_parse.py and fb_sql.py, and move them into the unzipped Facebook data folder. In the command line, run fb_parse.py and follow the prompts.
## Dependancies
This was written in Python 3.6, with numpy and matplotlib (>=2.0).
