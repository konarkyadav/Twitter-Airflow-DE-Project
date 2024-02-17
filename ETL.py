# Import necessary libraries
import tweepy  # To interact with the Twitter API
import pandas as pd  # For data manipulation and analysis
import json  # To work with JSON data
from datetime import datetime  # To work with dates and times
import s3fs  # To interact with Amazon S3

def run_twitter_etl():
    # Twitter API credentials
    access_key = "YOUR_ACCESS_KEY" 
    access_secret = "YOUR_ACCESS_SECRET" 
    consumer_key = "YOUR_CONSUMER_KEY"
    consumer_secret = "YOUR_CONSUMER_SECRET"

    # Authenticate with Twitter using OAuth
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)   
    auth.set_access_token(access_key, access_secret) 

    # Create a tweepy API object to fetch tweets
    api = tweepy.API(auth)

    # Fetch tweets from a specific user's timeline
    tweets = api.user_timeline(screen_name='@elonmusk', 
                               count=200,  # Maximum allowed count
                               include_rts=False,  # Exclude retweets
                               tweet_mode='extended'  # Fetch full text of the tweets
                              )

    # Initialize a list to hold refined tweets
    tweets_data = []

    # Loop through fetched tweets to extract relevant information
    for tweet in tweets:
        text = tweet._json["full_text"]  # Extract full text of the tweet

        # Create a dictionary with refined tweet details
        refined_tweet = {
            "user": tweet.user.screen_name,  # Twitter handle of the user
            'text': text,  # Full text of the tweet
            'favorite_count': tweet.favorite_count,  # Number of likes
            'retweet_count': tweet.retweet_count,  # Number of retweets
            'created_at': tweet.created_at  # Timestamp of the tweet creation
        }
        
        # Append the refined tweet to the list
        tweets_data.append(refined_tweet)

    # Convert the list of refined tweets into a DataFrame
    df_tweets = pd.DataFrame(tweets_data)
    
    # Save the DataFrame as a CSV file
    df_tweets.to_csv('refined_tweets.csv', index=False)
