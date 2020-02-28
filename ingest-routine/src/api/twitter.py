from external_lib import tweepy
import csv
import os
from utils.sns_obj import sns_notification
from utils.secrets_util import SecretsUtil
from utils.s3_util import S3Client
from datetime import datetime, timedelta

secrets_util = SecretsUtil()
SNS_FAILURE_TOPIC = 'TODO'
s3_client = S3Client()

class twitter:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

                   Arguments:
                       api_params-- Required parameters fetched from DynamoDB
                      source_id -- Uniqud id assigned to particular data source and file pattern
                   """
        # get required parameters from DynamoDB
        target_bucket = api_params['target_bucket']
        secret_manager_key = api_params['secret_manager_key']
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        # get secret keys from secret manager
        try:
            consumer_key = secrets_util.get_secret(secret_manager_key, 'consumer_key')
            consumer_secret = secrets_util.get_secret(secret_manager_key, 'consumer_secret')
            access_key = secrets_util.get_secret(secret_manager_key, 'access_key')
            access_secret = secrets_util.get_secret(secret_manager_key, 'access_secret')
        except Exception as e:
            # message = (
            #     f"Exception: {e} \nMessage: secret value cannot be fetched"
            #
            # )
            # sns_notification(
            #     SNS_FAILURE_TOPIC,
            #     message,
            #     "General exception occurred."
            # )

            print(f"Exception: {e} \nMessage: File google_analytics could not be"
                  f"uploaded to S3 bucket {target_bucket}"
                  "\nFunction name: google_analytics")

        api_params_source_filename = api_params["api_params_filename"]
        body = s3_client.read_file_from_s3(target_bucket, api_params_source_filename)
        json_view = eval(body)
        screen_name_list = json_view["twitter"]["username_list"]
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        bucket_key_folder = api_params['bucket_key_folder']
        # initializing the tweepy aut api
        api = tweepy.API(auth)
        file = '/tmp/tweets.csv'
        if os.path.exists(file):
            os.remove(file)

        # authorize twitter, initialize tweepy

        # initialize a list to hold all the tweepy Tweets

        row_list = []
        # looping through the screenames for getting the twitter details
        for i in screen_name_list:
            alltweets = []
            new_tweets = api.user_timeline(screen_name=i, count=200)

            # save most recent tweets
            alltweets.extend(new_tweets)

            # save the id of the oldest tweet less one
            oldest = alltweets[-1].id - 1

            # keep grabbing tweets until there are no tweets left to grab
            date = alltweets[0].created_at.strftime('%Y-%m-%d')
            twitter_id = alltweets[0].user.id
            twitter_name = alltweets[0].screen_name
            author = alltweets[0].user.user.name
            following_count = alltweets[0].user.friends_count
            follower_count = alltweets[0].user.followers_count
            listed_count = alltweets[0].user.listed_count
            favourites_count = alltweets[0].user.favourites_count
            tweets_count = alltweets[0].user.statuses_count
            # transform the tweepy tweets into a 2D array that will populate the csv
            outtweets = [date, twitter_id, twitter_name, author, follower_count, following_count, listed_count,
                         tweets_count, favourites_count]
            row_list.append(outtweets)
        for i in range(0, len(row_list)):
            row_list[i][0] = yesterday_date
        # write the csv
        try:
            # Writing the file to tmp folder
            with open('/tmp/tweets.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["DATE", "TWITTER_Id", "TWITTER_NAME", "AUTHOR", "FOLLOWERS_COUNT", "FOLLOWING_COUNT", "LISTED_IN",
                     "TWEETS_COUNT", "FAVOURITES_COUNT"])
                writer.writerows(row_list)
            # writing the file to s3 bucket
            s3_client.move_from_tmp_to_bucket(
                '/tmp/tweets.csv',
                bucket_key_folder + "/SOCIAL_MEDIA_TWITTER_{}.csv".format(yesterday_date),
                target_bucket
            )
        except Exception as e:
            # message = (
            #     f"Exception: {e} \nMessage: File youtube could not be"
            #     f"uploaded to S3 bucket {target_bucket}"
            #     "\nFunction name: yotube"
            # )
            # sns_notification(
            #     SNS_FAILURE_TOPIC,
            #     message,
            #     "General exception occurred."
            # )
            print(f"Exception: {e} \nMessage: Twitter data could not be"
                  f"uploaded to S3 bucket {target_bucket}"
                  "\nFunction name: twitter")





