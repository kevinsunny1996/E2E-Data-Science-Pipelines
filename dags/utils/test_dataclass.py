from dataclasses import asdict
from schema_handler import CSVFileSchema, SchemaField

input_file_schemas = [
    # Schema skeleton for ratings
    CSVFileSchema(
        # id,title,count,percent,game_id
        product_table_name="ratings",
        schema_fields=[
            SchemaField(name='id',type='INTEGER',mode='REQUIRED',description='Ratings ID corresponding to the rating given to a game'),
            SchemaField(name='title',type='STRING',mode='NULLABLE',description='Rating Type Corresponding to the respective Rating ID'),
            SchemaField(name='count',type='INTEGER',mode='NULLABLE',description='Number of games with the given rating , eg- Recommended for 30 games etc.'),
            SchemaField(name='percent',type='FLOAT',mode='NULLABLE',description='Percentage of games with the given rating'),
            SchemaField(name='game_id',type='INTEGER',mode='REQUIRED',description='Foreign key to map to gaming product table game ID')
        ]
    ),
    # Schema skeleton for games
    CSVFileSchema(
        # id,slug,name_original,description_raw,metacritic,released,tba,updated,rating,rating_top,playtime
        product_table_name="games",
        schema_fields=[
            SchemaField(name='id',type='INTEGER',mode='REQUIRED',description='Game ID for the given game'),
            SchemaField(name='slug',type='STRING',mode='NULLABLE',description='Slug version of the game name , eg- resident-evil-4'),
            SchemaField(name='name_original',type='STRING',mode='NULLABLE',description='Actual official name of the game'),
            SchemaField(name='description_raw',type='STRING',mode='NULLABLE',description='Game Description'),
            SchemaField(name='metacritic',type='FLOAT',mode='REQUIRED',description='Metacritic rating of the game'),
            SchemaField(name='released',type='DATE',mode='REQUIRED',description='Release date of the game in format YYYY-MM-DD'),
            SchemaField(name='tba',type='BOOLEAN',mode='REQUIRED',description='Is the game yet to be announced?'),
            SchemaField(name='updated',type='DATETIME',mode='REQUIRED',description='Time and date when the data was last updated'),
            SchemaField(name='rating',type='FLOAT',mode='REQUIRED',description='Rating of the game from 1 to 5'),
            SchemaField(name='rating_top',type='NUMERIC',mode='REQUIRED',description='Max Average Rating given to that game, relates to id of ratings table'),
            SchemaField(name='playtime',type='FLOAT',mode='REQUIRED',description='Playtime for the game in minutes')
        ]
    ),
    # Schema skeleton for genres
    CSVFileSchema(
        # id,name,slug,games_count,image_background,game_id
        product_table_name="genres",
        schema_fields=[
            SchemaField(name='id',type='INTEGER',mode='REQUIRED',description='Genre ID for the given game'),
            SchemaField(name='name',type='STRING',mode='NULLABLE',description='Name of the genre , eg- Adventure, Action etc.'),
            SchemaField(name='slug',type='STRING',mode='NULLABLE',description='Lower case name of the genre , eg- adventure, action etc.'),
            SchemaField(name='games_count',type='INTEGER',mode='NULLABLE',description='Count of games for that genre'),
            SchemaField(name='image_background',type='STRING',mode='REQUIRED',description='Image background URL'),
            SchemaField(name='game_id',type='INTEGER',mode='REQUIRED',description='Game ID , foreign key of Games table')
        ]
    ),
    # Schema skeleton for platforms
    CSVFileSchema(
        # released_at,platform_id,platform_name,platform_slug,platform_image,platform_year_end,platform_year_start,platform_games_count,platform_image_background,game_id
        product_table_name="platforms",
        schema_fields=[
            SchemaField(name='released_at',type='DATE',mode='NULLABLE',description='Release date of the game on the respective platform in format YYYY-MM-DD'),
            SchemaField(name='platform_id',type='INTEGER',mode='REQUIRED',description='Platform ID for the given platform'),
            SchemaField(name='platform_name',type='STRING',mode='REQUIRED',description='Platform Name'),
            SchemaField(name='platform_slug',type='STRING',mode='REQUIRED',description='Lower case platform name'),
            SchemaField(name='platform_image',type='STRING',mode='NULLABLE',description='Platform Image URL'),
            SchemaField(name='platform_year_end',type='FLOAT',mode='NULLABLE',description='End Year for the given platform'),
            SchemaField(name='platform_year_start',type='FLOAT',mode='NULLABLE',description='Start Year for the given platform'),
            SchemaField(name='platform_games_count',type='INTEGER',mode='NULLABLE',description='Count of games for that platform'),
            SchemaField(name='platform_image_background',type='STRING',mode='NULLABLE',description='Platform image background URL'),
            SchemaField(name='game_id',type='INTEGER',mode='REQUIRED',description='Game ID for the given game used as foreign key in games table')
        ]
    ),
    # Schema skeleton for publishers
    CSVFileSchema(
        # id,name,slug,games_count,image_background,game_id
        product_table_name="publishers",
        schema_fields=[
            SchemaField(name='id',type='INTEGER',mode='REQUIRED',description='Publisher ID for the given game'),
            SchemaField(name='name',type='STRING',mode='REQUIRED',description='Name of the publisher'),
            SchemaField(name='slug',type='STRING',mode='REQUIRED',description='Lower case name of the publisher'),
            SchemaField(name='games_count',type='INTEGER',mode='NULLABLE',description='Count of games for that publisher'),
            SchemaField(name='image_background',type='STRING',mode='NULLABLE',description='Image background URL'),
            SchemaField(name='game_id',type='INTEGER',mode='REQUIRED',description='Game ID for the given game used as foreign key in games table')
        ]
    )
]

def generate_schema_field_dicts():
    for schema in input_file_schemas:
        schema_field_dicts = [asdict(field) for field in schema.schema_fields]
        yield schema_field_dicts


if __name__=="__main__":
    schema_dicts_generator = generate_schema_field_dicts()
    print(next(schema_dicts_generator))
    # for schema_field_dicts in schema_dicts_generator:
        # print(f'{schema_field_dicts[0]},')
