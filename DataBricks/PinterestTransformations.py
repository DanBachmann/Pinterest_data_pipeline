from pyspark.sql.functions import *

def clean_pin(df_pin):
    df_pin = df_pin.replace({'': None})

    # Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
    #df_pin = df_pin.replace({'k': '000'}, subset=['follower_count']).replace({'M': '000000'}, subset=['follower_count'])  # ???: didn't work, but expected to
    df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
    df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

    # Ensure that each column containing numeric data has a numeric data type
    # Ignoring the download column since it is not used after the column reorder specification
    df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("integer"))
    df_pin = df_pin.withColumn("index", df_pin["index"].cast("long"))
    df_pin.printSchema()

    # Clean the data in the save_location column to include only the save location path
    #df['save_location'] = df['save_location'].apply(lambda x: x.replace('Local save in ','')
    #df_pin = df_pin.replace({'Local save in ': ''}, subset=['save_location']) # ???: didn't work, but expected to
    df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

    # Rename the index column to ind.
    df_pin = df_pin.withColumnRenamed("index", "ind")

    # Reorder the DataFrame columns to have the following column order:
        #     ind
        #     unique_id
        #     title
        #     description
        #     follower_count
        #     poster_name
        #     tag_list
        #     is_image_or_video
        #     image_src
        #     save_location
        #     category
    df_pin = df_pin[['ind', 'unique_id', 'title', 'description', 'follower_count','poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category']]
    return df_pin

def clean_geo(df_geo):
    # Create a new column coordinates that contains an array based on the latitude and longitude columns
    df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

    # Drop the latitude and longitude columns from the DataFrame
    df_geo = df_geo.drop("latitude", "longitude")
    #df_geo.show(10)

    # Convert the timestamp column from a string to a timestamp data type
    df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
    df_geo.printSchema()

    # Reorder the DataFrame columns to have the following column order:
        # ind
        # country
        # coordinates
        # timestamp
    df_geo = df_geo['ind', 'country', 'coordinates', 'timestamp']
    return df_geo

def clean_user(df_user):
    # Create a new column user_name that concatenates the information found in the first_name and last_name columns
    df_user = df_user.withColumn("user_name", concat("first_name", "last_name"))

    # Drop the first_name and last_name columns from the DataFrame
    df_user = df_user.drop("first_name", "last_name")

    # Convert the date_joined column from a string to a timestamp data type
    df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
    df_user.printSchema()

    # Reorder the DataFrame columns to have the following column order:
    #     ind
    #     user_name
    #     age
    #     date_joined
    df_user = df_user['ind', 'user_name', 'age', 'date_joined']
    return df_user
