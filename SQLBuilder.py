import sqlite3
import pandas as pd
import ast
from sqlalchemy import create_engine
from datetime import datetime
df = pd.read_excel("recipes.xlsm")
reviews_df = pd.read_csv("reviews.csv")

def safe_literal_eval(value):
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError) as e:
        return []  # Return an empty list for invalid or problematic strings

print(df.head())
print(df.dtypes)

df['RecipeIngredientParts'] = df['RecipeIngredientParts'].apply(lambda x: safe_literal_eval(x) if pd.notna(x) else [])
df['RecipeIngredientQuantities'] = df['RecipeIngredientQuantities'].apply(lambda x: safe_literal_eval(x) if pd.notna(x) else [])
df['RecipeInstructions'] = df['RecipeInstructions'].apply(lambda x: safe_literal_eval(x) if pd.notna(x) else [])
df['RecipeInstructions'] = df['RecipeInstructions'].apply(lambda x: '; '.join(x))

def convert_datetime(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value

# Convert datetime columns if they exist
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, datetime)).any():
        df[col] = df[col].apply(convert_datetime)

# Format the 'Images' column to store only the first element or None if the list is empty
def process_images(image_list):
    if not image_list:  # If the list is empty
        return None      # Return None for empty lists
    else:
        return image_list[0]  # Return the first element in the list if it exists

# Apply the function to the 'Images' column
df['Images'] = df['Images'].apply(lambda x: safe_literal_eval(x) if pd.notna(x) else [])  # Convert string representations to lists
df['Images'] = df['Images'].apply(process_images)

recipes_df = df[['RecipeId','Name','AuthorId','AuthorName','CookTime','PrepTime','TotalTime','DatePublished','Description','Images','RecipeCategory','Keywords','AggregatedRating','ReviewCount','Calories','FatContent','SaturatedFatContent','CholesterolContent','SodiumContent','CarbohydrateContent','FiberContent','SugarContent','ProteinContent','RecipeYield','RecipeInstructions']].drop_duplicates()

print("DataFrames Loaded")
recipe_ingredients_list = []
for index, row in df.iterrows():
    recipe_id = row['RecipeId']
    print(recipe_id)
    ingredients = row['RecipeIngredientParts']
    quantities = row['RecipeIngredientQuantities']
    for ingredient, quantity in zip(ingredients, quantities):
        recipe_ingredients_list.append({
            'RecipeId': recipe_id,
            'Ingredient': ingredient,
            'Quantity': quantity
        })
recipe_ingredients_df = pd.DataFrame(recipe_ingredients_list)

# Create an SQLite engine (or any other database engine)
engine = create_engine('sqlite:///recipes.db')

def create_tables(connection):
    cursor = connection.cursor()
    
    # Create 'recipes' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS recipes (
        recipe_id INTEGER PRIMARY KEY,
        name TEXT,
        author_id INTEGER,
        author_name TEXT,
        cook_time INTEGER,
        prep_time INTEGER,
        total_time INTEGER,
        date_published TEXT,
        description TEXT,
        images TEXT,
        recipe_category TEXT,
        keywords TEXT,
        aggregated_rating REAL,
        review_count INTEGER,
        calories REAL,
        fat_content REAL,
        saturated_fat_content REAL,
        cholesterol_content REAL,
        sodium_content REAL,
        carbohydrate_content REAL,
        fiber_content REAL,
        sugar_content REAL,
        protein_content REAL,
        recipe_yield TEXT,
        recipe_instructions TEXT
    )
    ''')

    # Create 'ingredients' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ingredients (
        ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ingredient_name TEXT UNIQUE
    )
    ''')

    # Create 'recipe_ingredients' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS recipe_ingredients (
        recipe_id INTEGER,
        ingredient_id INTEGER,
        quantity TEXT,
        FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
        FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id)
    )
    ''')

    #Create 'reviews' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reviews (
        review_id INTEGER PRIMARY KEY,
        recipe_id INTEGER,
        author_id INTEGER,
        author_name TEXT,
        rating REAL,
        review TEXT,
        date_submitted TEXT,
        date_modified TEXT,
        FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id)
    )
    ''')

print("Creating SQL Tables")
# Create the tables
with sqlite3.connect("recipes.db") as connection:
    create_tables(connection)

# Insert data into recipes table
recipes_df.to_sql('recipes', engine, if_exists='replace', index=False)

# Get unique ingredients and insert them into the 'ingredients' table
unique_ingredients = recipe_ingredients_df[['Ingredient']].drop_duplicates()
unique_ingredients.columns = ['ingredient_name']  # Rename column for consistency
unique_ingredients.to_sql('ingredients', engine, if_exists='replace', index=False)

# Fetch ingredient IDs after insertion
ingredients_with_ids = pd.read_sql('SELECT * FROM ingredients', engine)

# Merge ingredient IDs into recipe_ingredients_df
recipe_ingredients_df = recipe_ingredients_df.merge(ingredients_with_ids, left_on='Ingredient', right_on='ingredient_name')

# Insert data into recipe_ingredients table
recipe_ingredients_df[['RecipeId', 'Ingredient', 'Quantity']].to_sql('recipe_ingredients', engine, if_exists='replace', index=False)

# Insert data into reviews table
reviews_df.to_sql('reviews', engine, if_exists='replace', index=False)

print("Tables created and data inserted successfully!")