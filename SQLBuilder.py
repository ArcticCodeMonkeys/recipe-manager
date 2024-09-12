import sqlite3
import pandas as pd
connection = sqlite3.connect("RecipeInfo.db")
cursor = connection.cursor()

df = pd.read_excel("recipe.xlsm")
for index, row in df.iterrows():
    parts = list(f"{row['IngredientParts']}")
    quantities = list(f"{row['IngredientQuantities']}")
cursor.executescript("CREATE TABLE IF NOT EXISTS ingredients (id SERIAL PRIMARY KEY,item_number VARCHAR(10),item_name VARCHAR(255));")

connection.commit()