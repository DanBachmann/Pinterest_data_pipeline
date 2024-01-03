from pyspark.sql.functions import *

def clean_pin(df_pin):
    # Replace empty entries and entries with no relevant data in each column with Nones
    df_pin = df_pin.replace({'': None})
    # TODO: clear out error entry columns (no relevant data)

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
