# Here I import the packages I will need to make the request, get the request into a usable
# format, perform RegEx validation on the data, and then create DataFrames from that data
import requests
import json
import re
import pandas as pd

# This while loop accepts user input in the form of a player name and performs validation on that
# input if numbers or special characters are input. The validation also covers the neccessary 
# input format: First Last. The validation then capitalizes the first letter for use with the
# response.
while True:
    player_data = input('What player would you like to create a card for? (Please enter full name): ')
    if re.match(r"[\s\w]+$", player_data):
        player_data = player_data.title()
        break
    print('ERROR: Numbers are not valid input. Try again.')


#I set some default parameters here to show variability in where the API_URI can be located
def get_player_data(player_data, endpoint='players', base_url='https://statsapi.mlb.com/api/v1/sports/1/'):

    # This make a get request to the api, uses the json package to transform the 'requests.object'
    # into a usable JSON format

    player_uri = base_url+endpoint

    player_data_request = requests.get(player_uri)

    player_data_response = player_data_request.json()

    # Because the JSON response is a nested list of dictionaries, I use this notation to get the values of
    # the response where the list of dictionaries is located. The key:value pair is people:values

    players = player_data_response['people']    

    # This will filter the player response data down to only the player where the 'fullName' matches
    # the user input variable that is passed

    this_player = [player for player in players if player['fullName'] == player_data]    

    return this_player


# This will get the necessary person/bio data from the player JSON
def parse_general_data(player):

    # These are the only values we want 
    stats = ['id', 'lastName', 'firstName', 'height', 'weight', 'birthDate', 'birthCity', 'birthStateProvince', 'birthCountry', 'primaryNumber', 'mlbDebutDate']

    # This is using a while loop generator to append the matching 'stats' labels above (in the list) to
    # the location within the JSON data that is passed in the function as 'player'
    i = 0
    bio_stats = []
    while len(stats) > i:
        for item in player:
            bio_stats.append(item.get(stats[i], None))
            i += 1
    
    # This creates a dictionary of the 'stats' with the returned values within the 'bio_stats' list
    bio_stats_dict = {key:value for key, value in zip(stats, bio_stats)}

    # These are extra stats that are nested dictionaries and this requires a different method of extraction
    extra_bio_stats = ['position', 'batSide', 'pitchHand']
    extra_stats = []

    # This for loop performs a more verbose method of data parsing, but is similar to the method above
    # I opted to use this to show another method of getting the data. Typically, I would avoid this type 
    # of notation, as it is computationally slower (by a small margin)
    for item in player:
        position = item.get('primaryPosition', None)
        position = position['abbreviation']
        extra_stats.append(position)

        batSide = item.get('batSide', None)
        batSide = batSide['description']
        extra_stats.append(batSide)

        pitchHand = item.get('pitchHand', None)
        pitchHand = pitchHand['description']
        extra_stats.append(pitchHand)

    # This zips/creates the dictionary we need
    extra_stats_dict = {key:value for key, value in zip(extra_bio_stats, extra_stats)}
    
    # Here we update the dictionary, essentially performing an append to the 'bio_Stats_dict' which is
    # the master dict
    bio_stats_dict.update(extra_stats_dict)

    return bio_stats_dict


# This function uses the 'education' hydration to get player education data as it exists
def parse_education_data(player_id, hydrate='education'):

    # Opted to use an f string notation to remain flexible to data_input
    education_uri = f'https://statsapi.mlb.com/api/v1/people/{player_id}/?hydrate={hydrate}'

    # This section performs the request, response data type transformation, and relative_path
    # locator as mentioned above
    education_request = requests.get(education_uri)

    education_response = education_request.json()

    edu = education_response['people']

    # This simple for loop gets the 'edu_player_id' that will be used to merge this
    # education DataFrame to the player/person DataFrame later
    for item in edu:
        edu_player_id = item['id']

    # These are the empty dicts in the case where there is no information to return
    # The master dict gets the 'edu_player_id' value by default
    highschool_dict = {}
    college_dict = {}
    master_dict = {'edu_player_id': edu_player_id}

    # This for loop will iterate through the player data response that is stored
    # in 'edu' and get each individual dict object from that list of dicts
    for item in edu:

        # This gets the education dict (which is a nested dict inside a list of dicts)
        education = item.get('education', None)

        for key in education:

            # For each key:value pair in the education dict, we 
            # are going to see if the player has education data
            # Alternatively, a try-except could be used for error
            # handling. That seemed unnecessary at this time.
            if education == None:
                return print('This player has no education history.')
        
            # If the key of the key:value pair is 'highschool' and the length of
            # that dict is not 0, then get the name, city, state of the high school and append it to a list
            if (key == 'highschools') and (len(education['highschools']) != 0):
                highschool_items = ['highschool_name', 'city', 'state']
                highschool_values = []

                highschool = education['highschools'][0]

                highschool_values.append(highschool['name'])
                highschool_values.append(highschool['city'])
                highschool_values.append(highschool['state'])

                # Here we take our lists and zip them to create a dict that is then appended to the master
                # dict and the if statement is pass, so the loop can continue to the next if statement
                highschool_dict = {key:value for key, value in zip(highschool_items, highschool_values)}
                master_dict.update(highschool_dict)
                continue

            # If the key in the key:value pair we are at is 'colleges' and colleges has a length
            # that is not 0, get the name of the college and create the dictionary, then
            # append that dictionary to the master dictionary
            if (key == 'colleges') and (len(education['colleges']) != 0):
                college_item = ['college_name']
                college_value = []

                college = education['colleges'][0]

                college_value.append(college['name'])

                college_dict = {key:value for key, value in zip(college_item, college_value)}
                master_dict.update(college_dict)
                continue

    return master_dict


# This is the award information that uses the 'awards' hydration for the given personId/player_id
def parse_award_data(player_id, hydrate='awards'):
    awards_uri = f'https://statsapi.mlb.com/api/v1/people/{player_id}/?hydrate={hydrate}'

    # Perform the request and data type transformation and relative path location
    awards_request = requests.get(awards_uri)

    awards_response = awards_request.json()

    player_awards = awards_response['people']

    awards_list = []

    # For each item in the list of dicts, use the relative path 'awards' and return that list of dicts
    for item in player_awards:

        list_of_awards = item.get('awards', None)

        # If that list of dicts actually returned something, get each individual dict item and put that into a list
        if list_of_awards is not None:

            award_for_player = [item for item in list_of_awards]

            # For each dict item, make a dictionary and append that dictionary to an empty list
            for award in award_for_player:

                awards = {'award_name': award['name'], 'award_season': award['season'], 'award_team_id': award['team']['id'], 'award_team_name': award['team']['teamName'], 'award_position': award['player']['primaryPosition']['abbreviation']}

                awards_list.append(awards)
    
    return awards_list


# Using the player_id/personId and the 'draft' hydration get the draft information of the player if it exists
def parse_draft_data(player_id, hydrate='draft'):

    draft_uri = f'https://statsapi.mlb.com/api/v1/people/{player_id}/?hydrate={hydrate}'

    draft_request = requests.get(draft_uri)

    draft_response = draft_request.json()

    player_draft = draft_response['people']

    drafts_list = []

    # Using the list of dicts, get each individual dict item and use the relative path 'drafts' to
    # assign that list of dicts to the variable 'list_of_drafts'
    for item in player_draft:

        list_of_drafts = item.get('drafts', None)

        # If that list of drafts actually returned something, parse out the individual dict items
        if list_of_drafts is not None:

            draft_item = [item for item in list_of_drafts]

            # Loop through each dict item from the 'drafts' relative path and append that to the list of 
            # Items in 'drafts_list'
            for draft in draft_item:

                drafts = {'draft_player_id': draft['person']['id'], 'draft_headshot_link': draft['headshotLink'], 'draft_team': draft['team']['name'], 'draft_year': draft['year'], 'draft_round': draft['pickRound'], 'draft_pick': draft['pickNumber']}

                drafts_list.append(drafts)

    return drafts_list


# Get the team data of the player using the 'currentTeam' hydration
def parse_team_data(player_id, hydrate='currentTeam'):

    current_team_uri = f'https://statsapi.mlb.com/api/v1/people/{player_id}/?hydrate={hydrate}'

    current_team_request = requests.get(current_team_uri)

    current_team_response = current_team_request.json()

    current_team_info = current_team_response['people']

    current_team_data = {}

    # Loop through the list of dicts and use the relative path 'currentTeam' to return that object
    for item in current_team_info:

        current_team = item.get('currentTeam', None)

        # If there is a current team, get the id to get the logo, and get the other necessary team information
        if current_team is not None:

            current_team_id = current_team['id']

            current_team_data = {'team_id': current_team_id, 'team_name': current_team['name'], 'team_logo': f'https://www.mlbstatic.com/team-logos/{current_team_id}.svg'}

    return current_team_data


# Get stats data using the arguments**: player_id and season
def parse_stats_data(player_id, season):

    # Convert the season into an integer that can be used in a while loop generator
    season = int(season)

    stats_per_season = []

    # While that season number is less than 2023, get the season stats
    while season < 2023:

        # Using the player_id/personId and the stats=season and group=pitching variables, get the 
        # stats starting with the season the player debuted
        season_stats = requests.get(f'https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&group=pitching&season={season}')

        season_stats_response = season_stats.json()

        season_stats_data = season_stats_response['stats']

        # For each item in the list of dicts from the reponse, use the relative path 'splits' to get the 
        # stats information that we need
        for item in season_stats_data:

            list_of_season_stats = item.get('splits', None)

            # If there are stats data values to use from the relative path 'splits' get each item from that list of stats
            if list_of_season_stats is not None:

                stats_item = [item for item in list_of_season_stats]


                # This commented out code was used for testing, because of an issue with the 'team' relative path that is used below
                #listthis = [{stats['season']: stats['team']} for stats in stats_item]
                #print(listthis)
                
                
                # For each dict object, get the necessary information we need and append that dict of season stats to the master dict
                for stat in stats_item:
                    stats = {'Year': stat['season'], 'Team': stat['team']['name'], 'W': stat['stat']['wins'], 'L': stat['stat']['losses'], 'G': stat['stat']['gamesPlayed'], 'IP': stat['stat']['inningsPitched'], 'ERA': stat['stat']['era'], 'WHIP': stat['stat']['whip'], 'H': stat['stat']['hits'], 'R': stat['stat']['runs'], 'BB': stat['stat']['baseOnBalls'], 'SHO': stat['stat']['shutouts']}

                stats_per_season.append(stats)
        
        # increment the season number that will be used to get the next season stats
        season += 1

    return stats_per_season


# Here we run all of those functions using the 'general['id']' which is the playerId
# player_id is passed into all the functions as the argument that is set in the API_URI
my_player = get_player_data(player_data)
general = parse_general_data(my_player)
education = parse_education_data(general['id'])
award = parse_award_data(general['id'])
draft = parse_draft_data(general['id'])
team = parse_team_data(general['id'])

# Using the debutdate, get ONLY the year (which is always at the beginning) and set that year value
# to the variable 'start_season'. Pass 'start_season' as an argument to the stats_data function
start_season = general['mlbDebutDate'].split("-", 1)[0][-4:]
stats = parse_stats_data(general['id'], start_season)

# Here we unpack the dictionary using the items method
# This is done to turn the returned object into a DataFrame for easy reading
general = {key:[value] for key, value in general.items()}
general_df = pd.DataFrame.from_dict(general)


education = {key:[value] for key, value in education.items()}
education_df = pd.DataFrame.from_dict(education)

team = {key:[value] for key, value in team.items()}
team_df = pd.DataFrame.from_dict(team)


# Here, because the returned objects from 'award', 'draft', and 'stats' are lists of dicts
# We don't need to unpack anything and they can be passed using the from_records method
# Alternatively the vanilla `pd.DataFrame` method should return the same result
award_df = pd.DataFrame.from_records(award)
draft_df = pd.DataFrame.from_records(draft)
stats_df = pd.DataFrame.from_records(stats)


# This is the merge of the 'general_data'/bio_data and the 'education_data' into a single
# DataFrame object. I then remove the redundant player ids
person = general_df.merge(
    draft_df,
    left_on='id',
    right_on='draft_player_id',
    how='left'
).merge(
    education_df,
    left_on='id',
    right_on='edu_player_id',
    how='left'
).drop(
    columns=[
        'draft_player_id',
        'edu_player_id'
    ]
)

# Here, I write all of the DataFrames to CSVs using the 'player_data' f string to label each file
person.to_csv(f'{player_data}_person_data.csv')
stats_df.to_csv(f'{player_data}_stats_data.csv')
award_df.to_csv(f'{player_data}_awards_data.csv')
team_df.to_csv(f'{player_data}_team_data.csv')