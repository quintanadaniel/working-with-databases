import json

import pandas as pd
import sqlalchemy
import requests
import argparse


pokemon_url = "https://pokeapi.co/api/v2/pokemon/"
connection_url = "postgresql://postgres:password@postgres:5432/postgres"
db_engine = sqlalchemy.create_engine(connection_url)


def extract_pokemon(db_engine):
    raw_data = pd.read_sql("SELECT * FROM pokemons", db_engine)
    print(raw_data)
    return raw_data


def persist(data_frame, db_engine):
    data_frame.to_sql(
        name="pokemons",
        con=db_engine,
        index=False,
        if_exists="append" # We have three options append (add new register in the table), replace (replace the current register if exists) and fail (fail if exist in teh table)
    )


def parse_data_pokemon(data):
    return {
        "name": data["name"],
        "id": data["id"],
        "hp": next(item["base_stat"] for item in data["stats"] if item["stat"]["name"] == "hp"),
        "attack": next(item["base_stat"] for item in data["stats"] if item["stat"]["name"] == "attack"),
        "defense": next(item["base_stat"] for item in data["stats"] if item["stat"]["name"] == "defense"),
        "speed": next(item["base_stat"] for item in data["stats"] if item["stat"]["name"] == "speed"),
    }


if __name__ == "__main__":
    parse = argparse.ArgumentParser("Get pokemons stats by name")
    parse.add_argument("--name", type=str, help="Pokemon Name")
    args = parse.parse_args()

    response = requests.get(url=f"{pokemon_url}{args.name}")
    data = response.json()

    data_parse_pokemon = parse_data_pokemon(data)
    df_pokemon = pd.DataFrame([data_parse_pokemon])
    persist(df_pokemon, db_engine)

    raw_data_pokemons = extract_pokemon(db_engine)
    print(raw_data_pokemons)

