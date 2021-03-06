import time, os, datetime, sys, psutil, random, MySQLdb, statistics, requests, re
from bs4 import BeautifulSoup
import imageio

def say_hi():
    print("Hi, I'm ZackInc.py...")
    print("It is %s" % datetime.datetime.today())

def translate_event_type(p, pd, full_detail):
        e = "???"
        p = p.lower().strip()
        pd = pd.lower().strip()
        if pd.endswith("<"):
            pd = pd[0:-1]
        if p.strip() == "2-point shot":
            p = "shot"

        regex = re.compile('(\[[0-9]+:[0-9]+\]) .*?')
        match = regex.search(p.strip())
        if match is not None and match.start() == 0:
            p = p.replace(match.group(1), "").strip()

        if (p == "clear attempt" or (p.startswith("[") and "clear attempt" in p)) and pd == "good.":
            e = "Good Clear"
        elif p == "clear attempt" and pd == "failed.":
            e = "Failed Clear"
        elif p == "shot" and (" save " in pd or pd.startswith("save ")):
            e = "Saved Shot"
        elif p == "shot" and (" missed, " in pd or " wide " in pd or pd.endswith(" wide.") or pd.endswith(" missed.")  or pd.endswith(" wide") or pd.endswith(" high.") or pd.endswith(" high") or "high {" in pd):
            e = "Missed Shot"
        elif p == "shot" and (pd.endswith(" blocked.") or pd.endswith(" blocked") or " blocked {" in pd):
            e = "Blocked Shot"
        elif p == "shot" and ("hit post " in pd or pd.endswith(" hit post.") or pd.endswith(" hit post")):
            e = "Pipe Shot"
        elif p == "shot" and (pd.endswith(" hit crossbar") or pd.endswith(" hit crossbar.") or " hit crossbar {" in pd):
            e = "Pipe Shot"
        elif p == "goal" and " assist by " in pd:
            e = "Assisted Goal"
        elif p == "goal" and " assist by " not in pd:
            e = "Unassisted Goal"
        elif p == "turnover" and "(caused by " in pd:
            e = "Forced Turnover"
        elif "turnover" in p and p.startswith("[") and  "(caused by " in pd:
            e = "Forced Turnover"
        elif p.strip() == "turnover" and "(caused by " not in pd:
            e = "Unforced Turnover"
        elif "turnover" in p and p.startswith("[") and "(caused by " not in pd:
            e = "Unforced Turnover"
        elif p == "ground ball pickup":
            e = "Ground Ball"
        elif p.startswith("faceoff"):
            e = "Faceoff Win"
        elif p == "foul":
            e = "Penalty - 30 sec"
        elif p == "penalty" and "too many players/0:00" in pd:
            e = "Penalty - 30 sec"
        elif p == "penalty" and "minor/0:00" in pd:
            e = "Penalty - 30 sec"
        elif p == "penalty" and "pushing/0:00" in pd:
            e = "Penalty - 30 sec"
        elif p == "penalty" and "offside/0:00" in pd:
            e = "Penalty - 30 sec"
        elif p == "penalty" and "slsh/0:00" in pd:
            e = "Penalty - 1 min"
        elif p == "penalty" and "slashing/0:00" in pd:
            e = "Penalty - 1 min"
        elif p == "penalty" and "1:30" in pd:
            e = "Penalty - 90 sec"
        elif p == "penalty" and "0:30" in pd:
            e = "Penalty - 30 sec"
        elif p == "penalty" and "1:00" in pd:
            e = "Penalty - 1 min"
        elif p == "penalty" and "2:00" in pd:
            e = "Penalty - 2 min"
        elif p == "penalty" and "3:00" in pd:
            e = "Penalty - 3 min"
        elif p == "yellow card":
            e = "Penalty - 1 min"
        elif p == "red card":
            e = "Penalty - 3 min"
        elif "at goalie" in p:
            e = "Goalie Change"
        elif p == "timeout" or (p.startswith("[") and p.endswith("timeout")):
            e = "Timeout"
        elif p == "30-second clock warning" or p == "second clock warning" or p == "shot clock warning":
            e = "Shot Clock On"
        elif p == "30-second clock violation" or p == "second clock violation" or p == "shot clock violation":
            e = "Shot Clock Violation"
        elif " substitution" in p:
            e = "Substitution"
        elif "free position attempt" == p:
            e = None
        elif p == "shot":
            e = "Missed Shot"
        if e == "???" or False:
            print("Unable to assign the following play to any play type...\n\n")
            print("\t%s | %s\n" % (p, pd))
            print("\t%s" % (full_detail))
            sys.exit()
        return e

def translate_confirmed_teams(home_team_full, away_team_full, alt_team1, alt_team2):
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    query = "SELECT confirmed_home_team, count(1) from LaxRef_Games where home_team=%s and not ISNULL(home_team) and not ISNULL(confirmed_home_team) group by confirmed_home_team, home_team order by 2 desc"
    param = [alt_team1]
    cursor.execute(query, param)
    res = cursor.fetchall()
    home_teams = []
    home_team_counts = []
    #print("Home Team Options\n---------------------------------")
    for r in res:
        print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT confirmed_home_team, count(1) from LaxRef_Games where alt_home_team=%s and not ISNULL(alt_home_team) and not ISNULL(confirmed_home_team) group by confirmed_home_team, alt_home_team order by 2 desc"
    param = [alt_team1]
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Home Team Options\n---------------------------------")
    for r in res:
        print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT confirmed_away_team, count(1) from LaxRef_Games where away_team=%s and not ISNULL(away_team) and not ISNULL(confirmed_away_team) group by confirmed_away_team, away_team order by 2 desc"
    param = [alt_team2]
    cursor.execute(query, param)
    res = cursor.fetchall()
    away_teams = []
    away_team_counts = []
    #print("Away Team Options\n---------------------------------")
    for r in res:
        print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT confirmed_away_team, count(1) from LaxRef_Games where alt_away_team=%s and not ISNULL(alt_away_team) and not ISNULL(confirmed_away_team) group by confirmed_away_team, alt_away_team order by 2 desc"
    param = [alt_team2]
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Away Team Options\n---------------------------------")
    for r in res:
        print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    cursor.close(); mysql_conn.close()
    alt_away_team = None
    if len(away_team_counts) == 0:
        print("Error, no away team options found.")
    elif len(away_team_counts) == 1:
        alt_away_team = away_teams[0]
    elif len(away_team_counts) == 2:
        if away_team_counts[0] * 100 / sum(away_team_counts) > 66:
            alt_away_team = away_teams[0]
        else:
            print("There were 2 options, but it was pretty close, so I'm not sure...\n\t%s\n\t%s\n" % (str(away_teams), str(away_team_counts)))
    else:
        std_dev = statistics.stdev(away_team_counts)
        avg = statistics.mean(away_team_counts)
        if (away_team_counts[0] - avg) / std_dev > 2:
            alt_away_team = away_teams[0]
        else:
            print("First away option std dev was only %.2f" % ((away_team_counts[0] - avg) / std_dev))
    alt_home_team = None
    if len(home_team_counts) == 0:
        print("Error, no home team options found.")
    elif len(home_team_counts) == 1:
        alt_home_team = home_teams[0]
    elif len(home_team_counts) == 2:
        if home_team_counts[0] * 100 / sum(home_team_counts) > 66:
            alt_home_team = home_teams[0]
        else:
            print("There were 2 options, but it was pretty close, so I'm not sure...\n\t%s\n\t%s\n" % (str(home_teams), str(home_team_counts)))
    else:
        std_dev = statistics.stdev(home_team_counts)
        avg = statistics.mean(home_team_counts)
        if (home_team_counts[0] - avg) / std_dev > 2:
            alt_home_team = home_teams[0]
        else:
            print("First home option std dev was only %.2f" % ((home_team_counts[0] - avg) / std_dev))

    return alt_home_team, alt_away_team

def translate_alt_teams(home_team_full, away_team_full, alt_team1, alt_team2, print_it=True):
    if print_it:
        print("Translate Alt Teams\n-----------------------------\n\t Home team full: %s\n\t Away Team Full: %s\n\t Alt Team 1: %s\n\t Alt Team 2: %s\n" % (home_team_full, away_team_full, alt_team1, alt_team2))
    mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    query = "SELECT IFNULL(home_team, ''), count(1) from LaxRef_Games where not isnull(home_team) and home_team=%s group by home_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    home_teams = []
    home_team_counts = []
    away_teams = []
    away_team_counts = []
    #print("Alt Team 1 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(confirmed_home_team, ''), count(1) from LaxRef_Games where not isnull(confirmed_home_team) and  home_team=%s group by confirmed_home_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 2 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(home_team, ''), count(1) from LaxRef_Games where not isnull(home_team) and  home_team=%s group by home_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 2 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(home_team, ''), count(1) from LaxRef_Games where not isnull(home_team) and  alt_home_team=%s group by home_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()

    #print("Alt Team 3 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(home_team, ''), count(1) from LaxRef_Games where not isnull(home_team) and  alt_home_team=%s group by home_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 4 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(confirmed_home_team, ''), count(1) from LaxRef_Games where not isnull(confirmed_home_team) and  alt_home_team=%s group by confirmed_home_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 4 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(confirmed_home_team, ''), count(1) from LaxRef_Games where not isnull(confirmed_home_team) and  alt_home_team=%s group by confirmed_home_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 4 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(confirmed_home_team, ''), count(1) from LaxRef_Games where not isnull(confirmed_home_team) and  alt_home_team=%s group by confirmed_home_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 4 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(alt_home_team, ''), count(1) from LaxRef_Games where not isnull(alt_home_team) and  home_team=%s group by alt_home_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 5 Home Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]

    query = "SELECT IFNULL(away_team, ''), count(1) from LaxRef_Games where not isnull(away_team) and  away_team=%s group by away_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 1 Away Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(away_team, ''), count(1) from LaxRef_Games where not isnull(away_team) and  away_team=%s group by away_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 2 Away Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(away_team, ''), count(1) from LaxRef_Games where not isnull(away_team) and  alt_away_team=%s group by away_team order by 2 desc"
    param = [alt_team1]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 3 Away Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in home_teams:
            home_teams.append(r[0])
            home_team_counts.append(0)
        home_team_counts[home_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(away_team, ''), count(1) from LaxRef_Games where not isnull(away_team) and  alt_away_team=%s group by away_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 4 Away Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    query = "SELECT IFNULL(alt_away_team, ''), count(1) from LaxRef_Games where not isnull(alt_away_team) and  away_team=%s group by alt_away_team order by 2 desc"
    param = [alt_team2]
    #print("\nQuery %s w/ %s" % (query, param))
    cursor.execute(query, param)
    res = cursor.fetchall()
    #print("Alt Team 5 Away Team Options\n---------------------------------")
    for r in res:
        #print("%s - %d" % (r[0], r[1]))
        if r[0] not in away_teams:
            away_teams.append(r[0])
            away_team_counts.append(0)
        away_team_counts[away_teams.index(r[0])] += r[1]
    cursor.close(); mysql_conn.close()

    alt_away_team = None
    alt_home_team = None
    if print_it:
        print("Alt Team 2 Options\n---------------------------------")
    for team, count in zip(away_teams, away_team_counts):
        if print_it:
            print("%s - %d" % (team, count))
        if team.title() == home_team_full.title():
            alt_home_team = alt_team2
            #print("Because %s matched %s, set the alt_home_team to %s" % (team, home_team_full, alt_home_team))
            break
        if team.title() == away_team_full.title():
            alt_away_team = alt_team2
            #print("Because %s matched %s, set the alt_away_team to %s" % (team, away_team_full, alt_away_team))
            break

    if print_it:
        print("Alt Team 1 Options\n---------------------------------")
    for team, count in zip(home_teams, home_team_counts):
        if print_it:
            print("%s - %d" % (team, count))
        if team.title() == home_team_full.title():
            alt_home_team = alt_team1
            #print("Because %s matched %s, set the alt_home_team to %s" % (team, home_team_full, alt_home_team))
            break
        if team.title() == away_team_full.title():
            alt_away_team = alt_team1
            #print("Because %s matched %s, set the alt_away_team to %s" % (team, away_team_full, alt_away_team))
            break


    if alt_home_team is not None and alt_away_team is None:

        alt_away_team = alt_team1 if alt_home_team == alt_team2 else alt_team2
        #print("Alt Home team was %s, set alt_away_team to %s" % (alt_home_team, alt_away_team))
    elif alt_home_team is None and alt_away_team is not None:

        alt_home_team = alt_team1 if alt_away_team == alt_team2 else alt_team2
        #print("Alt Away team was %s, set alt_home_team to %s" % (alt_away_team, alt_home_team))
    return alt_home_team, alt_away_team

def get_laxpower_forum(cursor, team):
    query = "SELECT twitter_handle from GA_Tracking_Tags where email_campaign_descriptor like %s and campaign_source='LaxPower Forums'"
    param = ["%s%%" % team]
    cursor.execute(query, param)
    res = cursor.fetchone()
    if res is not None:
        res = res[0]
    return res

def get_IL_win_pct():
    url = "http://insidelacrosse.com/league/DI/teams"


    print("Make request...")
    response = requests.get(url, timeout=20)
    #print(response.content)
    regex = re.compile(r'<tr><td><a href\=\"\/team\/.*?\">(.*?)</a></td><td>([0-9]+)</td><td>([0-9]+)</td>')
    matches = re.findall(regex, response.content)
    total_wins = 0
    total_losses = 0
    IL_teams = []
    for m in matches:
        print("Match: %s has %s wins and %s losses" % m)
        team_name = m[0]
        team_name = "Detroit" if team_name == "Detroit Mercy" else team_name
        team_name = "Boston U" if team_name == "Boston University" else team_name
        team_name = "Massachusetts-Lowell" if team_name == "UMass Lowell" else team_name
        team_name = "Massachusetts" if team_name == "UMass" else team_name
        team_name = "Mount St Marys" if team_name == "Mount St Mary's" else team_name
        team_name = "Hobart and William" if team_name == "Hobart" else team_name
        team_name = "Loyola MD" if team_name == "Loyola" else team_name
        team = {'team': team_name, 'win_pct': float(m[1])/(float(m[1]) + float(m[2]))}
        IL_teams.append(team)
    return IL_teams

def get_adj_odds(section, adjusted_diff, win_odds_data, win_odds_ID):
    odds = 0
    loc = 22 + adjusted_diff
    loc = max(0, min(loc, 41))


    if section in win_odds_ID:
        last = -1
        for i, o_ in enumerate(win_odds_data[win_odds_ID.index(section)].split(",")[:-1]):
            if o_ != "" and last != "":
                o = float(o_)
                next_val = win_odds_data[win_odds_ID.index(section)].split(",")[i+1]
                point = (loc-i)
                if loc > i and loc <= i+1:
                    if o == 1.0:
                        return o
                    elif o == 0.0:
                        if next_val == "":
                            next_val = 0
                        #print("Next Val: %s\to: %s\tpoint: %s" % (str(next_val),str(o),str(point)))
                        odds =  (float(next_val) - float(o))*point + float(o)
                        return odds
                    else:
                        try:
                            odds =  (float(next_val) - float(o))*point + float(o)
                        except ValueError:
                            print("In Get Adj Odds: Error converting %s & %s" % (next_val, o))
                            sys.exit()
                        return odds

        return -1
    else:

        return  -1

current_milli_time = lambda: int(round(time.time() * 1000))

def get_tracking_name(tracking_code, cursor):
    query = "SELECT email_campaign_descriptor from GA_Tracking_Tags where RTRIM(LTRIM(campaign_content))=%s"
    param = [tracking_code.strip()]
    print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    r = cursor.fetchone()
    if r is None:
        if tracking_code != "(not set)":
            print("Returned nothing for %s" % tracking_code)
        return tracking_code
    else:
        return r[0]

def get_laxref_standings(yr, cursor=None):
    reconnect = False
    if cursor is None:
        reconnect = True
        mysql_conn, r = mysql_connect(); cursor = mysql_conn.cursor()
    url = "http://insidelacrosse.com/league/DI/teams"
    if yr is not None:
        url += "/%d" % (yr - 2000)
    else:
        yr = datetime.datetime.today().year

    print("Make request...")
    response = requests.get(url, timeout=20)
    #print(response.content)
    regex = re.compile(r'<tr><td><a href\=\"\/team\/.*?\">(.*?)</a></td><td>([0-9]+)</td><td>([0-9]+)</td>')
    matches = re.findall(regex, response.content)
    total_wins = 0
    total_losses = 0
    teams = []
    for m in matches:
        print("Match: %s has %s wins and %s losses" % m)
        team_name = m[0]
        team_name = "Detroit" if team_name == "Detroit Mercy" else team_name
        team_name = "Boston U" if team_name == "Boston University" else team_name
        team_name = "Massachusetts-Lowell" if team_name == "UMass Lowell" else team_name
        team_name = "Massachusetts" if team_name == "UMass" else team_name
        team_name = "Mount St Marys" if team_name == "Mount St Mary's" else team_name
        team_name = "Hobart and William" if team_name == "Hobart" else team_name
        team_name = "Loyola MD" if team_name == "Loyola" else team_name
        team = {'team': team_name, 'IL_wins': int(m[1]), 'IL_losses': int(m[2]), 'LR_wins': 0, 'LR_losses': 0, 'missing': 0}
        teams.append(team)
        total_wins += int(m[1])
        total_losses += int(m[2])
    print("\nTotal record: %d - %d" % (total_wins, total_losses))

    query = "SELECT count(1) from LaxRef_Games where league='NCAA Men' and year(game_date) = 2017"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    print("The database actually contains %d games from 2017." % (count))

    query = "SELECT confirmed_home_team, count(1) from LaxRef_Games where IFNULL(game_type,'')='' and league='NCAA Men' and not isnull(confirmed_home_team) and not isnull(confirmed_away_team) and  home_score > away_score and year(game_date) = %s and active=1 group by confirmed_home_team"
    param = [yr]
    cursor.execute(query, param)
    res = cursor.fetchall()
    for r in res:

        if r[0] != "Hampton":
            found = False
            for t in teams:

                    if t['team'].strip() == r[0].strip():
                        t['LR_wins'] += r[1]
                        found = True
                        break
            if not found:
                teams.append({'team': r[0], 'IL_wins': 0, 'IL_losses': 0, 'LR_wins': r[1], 'LR_losses': 0, 'missing': 0})


    query = "SELECT confirmed_home_team, count(1) from LaxRef_Games where league='NCAA Men' and not isnull(confirmed_home_team) and not isnull(confirmed_away_team) and home_score < away_score and year(game_date) = %s and active=1 group by confirmed_home_team"
    param = [yr]
    cursor.execute(query, param)
    res = cursor.fetchall()
    for r in res:
        if r[0] == "North Carolina":
            print("C: %s" % str(r))
        if r[0] != "Hampton":
            found = False
            for t in teams:
                if t['team'].strip() == r[0].strip():
                    t['LR_losses'] += r[1]
                    found = True
                    break
            if not found:
                teams.append({'team': r[0], 'IL_wins': 0, 'IL_losses': 0, 'LR_wins': 0, 'LR_losses': r[1], 'missing': 0})

    query = "SELECT confirmed_away_team, count(1) from LaxRef_Games where league='NCAA Men' and not isnull(confirmed_home_team) and not isnull(confirmed_away_team) and home_score < away_score and year(game_date) = %s and active=1 group by confirmed_away_team"
    param = [yr]
    cursor.execute(query, param)
    res = cursor.fetchall()
    for r in res:
        if r[0] == "North Carolina":
            print("C: %s" % str(r))
        if r[0] != "Hampton":
            found = False
            for t in teams:
                if t['team'].strip() == r[0].strip():
                    t['LR_wins'] += r[1]
                    found = True
                    break
            if not found:
                teams.append({'team': r[0], 'IL_wins': 0, 'IL_losses': 0, 'LR_wins': r[1], 'LR_losses': 0, 'missing': 0})

    query = "SELECT confirmed_away_team, count(1) from LaxRef_Games where league='NCAA Men' and not isnull(confirmed_home_team) and not isnull(confirmed_away_team) and home_score > away_score and year(game_date) = %s and active=1 group by confirmed_away_team"
    param = [yr]
    cursor.execute(query, param)
    res = cursor.fetchall()
    for r in res:
        if r[0] == "Brown":
            print("D: %s" % str(r))
        if r[0] != "Hampton":
            found = False
            for t in teams:
                if t['team'].strip() == r[0].strip():
                    t['LR_losses'] += r[1]
                    found = True
                    break
            if not found:
                teams.append({'team': r[0], 'IL_wins': 0, 'IL_losses': 0, 'LR_wins': 0, 'LR_losses': r[1], 'missing': 0})
    if reconnect:
        cursor.close(); mysql_conn.close()
    return teams

def get_team_gif(team):
    tteam = team.replace("North Carolina", "UNC").replace("Johns Hopkins", "JHU").replace("Penn State", "PSU").replace("Ohio State", "OSU").lower()
    if os.path.isfile(os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.gif" % tteam)):
        return os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.gif" % tteam)
    elif os.path.isfile(os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.jpg" % tteam)):
        return os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.jpg" % tteam)
    elif os.path.isfile(os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.png" % tteam)):
        return os.path.join("/home/pi/zack/LacrosseReference/TeamLogos", "%s.png" % tteam)

    return None

def get_twitter_tracking_tags(cursor):
    cursor.execute("Select twitter_handle, campaign_content from GA_Tracking_Tags where campaign_source='twitter'")
    results = []
    res = cursor.fetchall()
    for r in res:
        results.append({'handle': r[0],'code': r[1]})
    return results

def get_season_value(cursor, year, team):
    query = "SELECT team, url_week, votes from LaxRef_Rankings where team=%s and year=%s and active=1 order by week asc"
    param = [team, year - 2000]
    cursor.execute(query, param)
    res = cursor.fetchall()
    exponential = 1
    inc = .08
    total_season_value = 0
    for r in res:
        total_season_value += r[2] * exponential
        exponential *= (1 + inc)

    return total_season_value

def get_team_url(cursor):
    cursor.execute("Select name, IL_url from LaxRef_Teams where active=1")
    results = []
    res = cursor.fetchall()
    for r in res:
        results.append({'Team': r[0],'url': r[1]})
    return results

def get_twitter_handles(cursor):
    cursor.execute("Select name, IFNULL(twitter_handle, '') from LaxRef_Teams where active=1")
    results = []
    res = cursor.fetchall()
    for r in res:
        results.append({'Team': r[0],'handle': r[1]})
    return results

def get_team_colors(cursor):
    cursor.execute("Select name, IFNULL(fg_color, '(255,255,255,255)'), IFNULL(bg_color, '(0,0,0,255)') from LaxRef_Teams where active=1")
    results = []
    res = cursor.fetchall()
    for r in res:
        results.append({'Team': r[0],'fg_color': r[1],'bg_color': r[2]})
    return results

def get_url_title(url, cursor):

    query = "SELECT title from LaxRef_Articles where (url=%s or url=%s or url=%s) and active=1"
    param = ["http://lacrossereference.com%s" % url, "http://lacrossereference.com%s" % url[0:-1], "http://lacrossereference.com%s/" % url]
    #print("Query %s w/ %s" % (query, param))
    cursor.execute(query, param)
    r = cursor.fetchone()
    if r is None:
        return url
    else:
        return r[0]

# Connect to Lacrosse Reference Database
def mysql_connect():
    cnx = None
    try:
        if True:
            # Connecting from an external network.
            # Make sure your network is whitelisted
            #logging.info("Connecting via remote %s..." % app.config['DBNAME'])
            client_cert_pem = "instance/client_cert_pem"
            client_key_pem = "instance/client_key_pem"
            ssl = {'cert': client_cert_pem, 'key': client_key_pem}

            host = "169.254.184.34"
            local_or_remote = open('/home/pi/zack/local_or_remote', 'r').read()
            if local_or_remote == "remote":
                host = "127.0.0.1"

            #print("Connect on %s" % host)
            cnx = MySQLdb.connect(
                host='77.104.146.71',
                port=3306,
                user='lacross9_zcuser', passwd='laxref101', db='lacross9_db101kozc', charset="utf8", use_unicode=True)

            #logging.info("Success = %s" % str(res[0]))
            response = "Success!"
    except Exception as err:
        response = "Failed."
        print("Connection error: %s" % err)

    return cnx, response

def gif(data_dict, filter_teams, filename):
    fig = plt.figure()
    ax1 = fig.add_axes((.1, .2, .8, .7))
    ax1.set_title("%s - %s" % (metric.title(), ys_['title']), loc='left')
    ax1.set_ylabel("# of %s" % metric.title())
    ax1.set_prop_cycle(cycler('color', ['r', 'g', 'b', 'y', 'c', 'm', 'k']))

    fontP = FontProperties()
    fontP.set_size('small')
    series = 0
    for y_, u in zip(ys_['vals'], urls):
        if sum(y_) > 0:
            print("For %s \n\tPlot %s vs \n\t%s" % (zc.get_url_title(u, cursor), str(x_dates), str(y_)))
            ax1.plot(x_dates, y_, label=zc.get_url_title(u, cursor), linewidth=2.0)
            series += 1
    if series > 0:
        plt.xticks(x_dates, dates)
        #ax1.legend(loc='best')
        #print("Shift it
        offset = -.3 - .04*series
        # -.50 is good for 5
        # -.70 is good for 10
        lgd = ax1.legend(bbox_to_anchor=(0,offset), loc='lower left', ncol=1)
        plt.savefig('/home/pi/zack/%sSummary (%s).png' % (metric.title(), ys_['title']), bbox_extra_artists=(lgd,), bbox_inches='tight');


    plt.close()
    for d in data_dict:
        continue
